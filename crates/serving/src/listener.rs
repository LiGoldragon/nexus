//! A bound socket that removes its own file, and only its own.

use std::os::unix::fs::MetadataExt;
use std::os::unix::net::UnixListener;
use std::path::PathBuf;

/// A listening Unix socket and the file it was bound at. Dropping it removes
/// the file only while it is still the one this listener bound, by device and
/// inode, so a successor's socket at the same path is left in place.
pub struct OwnedListener {
    listener: UnixListener,
    path: PathBuf,
    device: u64,
    inode: u64,
}

pub trait SocketBinding: Sized {
    /// Binds a listener at `path`.
    fn bind(path: PathBuf) -> std::io::Result<Self>;

    fn listener(&self) -> &UnixListener;
}

impl SocketBinding for OwnedListener {
    fn bind(path: PathBuf) -> std::io::Result<Self> {
        let listener = UnixListener::bind(&path)?;
        let metadata = std::fs::metadata(&path)?;
        Ok(Self {
            listener,
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    fn listener(&self) -> &UnixListener {
        &self.listener
    }
}

impl Drop for OwnedListener {
    fn drop(&mut self) {
        let still_ours = std::fs::metadata(&self.path)
            .map(|metadata| metadata.dev() == self.device && metadata.ino() == self.inode)
            .unwrap_or(false);
        if still_ours {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}
