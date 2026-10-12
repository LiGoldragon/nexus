use nexus::{Ancestry, Capturable, Identifiable, Process, Unidentified, Witnessing};
use std::{
    io::{Read, Write},
    os::unix::net::{UnixListener, UnixStream},
    process::{Child, Command},
    thread,
    time::{Duration, Instant},
};

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

#[test]
fn generated_process_association_and_archive_roundtrip() {
    fn requires_identifiable<T: Identifiable>(_: &T) {}
    let current = Process::current().unwrap();
    requires_identifiable(&current);
    let ancestry = current.identify().unwrap();
    assert_eq!(ancestry.first(), Some(&current));
    assert_eq!(ancestry.last().unwrap().pid, 1);
    let archive = rkyv::to_bytes::<rkyv::rancor::Error>(&ancestry).unwrap();
    let decoded = rkyv::from_bytes::<Ancestry, rkyv::rancor::Error>(&archive).unwrap();
    assert_eq!(decoded, ancestry);
}

#[test]
fn real_socket_cli_through_wrapper_keeps_the_parent_edge() {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("identity.sock");
    let listener = UnixListener::bind(&path).unwrap();
    listener.set_nonblocking(true).unwrap();
    let child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "wrapped_cli", "--nocapture"])
        .env("NEXUS_IDENTITY_FIXTURE_STAGE", "wrapper")
        .env("NEXUS_IDENTITY_FIXTURE_SOCKET", &path)
        .spawn()
        .unwrap();
    let wrapper_pid = i64::from(child.id());
    let mut child = ChildGuard(child);
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut socket = loop {
        match listener.accept() {
            Ok((socket, _)) => break socket,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < deadline, "owned fixture did not connect");
                assert!(
                    child.0.try_wait().unwrap().is_none(),
                    "wrapper exited early"
                );
                thread::sleep(Duration::from_millis(5));
            }
            Err(error) => panic!("accept: {error}"),
        }
    };
    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut size = [0; 4];
    socket.read_exact(&mut size).unwrap();
    let size = u32::from_le_bytes(size) as usize;
    assert!(size <= 1024 * 1024);
    let mut bytes = vec![0; size];
    socket.read_exact(&mut bytes).unwrap();
    let claim = rkyv::from_bytes::<Ancestry, rkyv::rancor::Error>(&bytes).unwrap();
    let witnessed = socket.witness(&claim).unwrap();
    assert_eq!(witnessed, claim);
    assert_ne!(claim[0].pid, wrapper_pid);
    assert_eq!(claim[1].pid, wrapper_pid);
    assert_eq!(claim[2].pid, i64::from(std::process::id()));
    assert_eq!(
        socket.witness(&claim[1..].to_vec()),
        Err(Unidentified::PeerMismatch)
    );
    let mut forged = claim.clone();
    forged[1] = claim[2].clone();
    assert_eq!(socket.witness(&forged), Err(Unidentified::Malformed));
    socket.write_all(&[1]).unwrap();
    assert!(child.0.wait().unwrap().success());
}

// Invoked as owned fixture subprocesses; inert in a normal test invocation.
#[test]
fn wrapped_cli() {
    let Ok(stage) = std::env::var("NEXUS_IDENTITY_FIXTURE_STAGE") else {
        return;
    };
    if stage == "wrapper" {
        let mut child = ChildGuard(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "wrapped_cli", "--nocapture"])
                .env("NEXUS_IDENTITY_FIXTURE_STAGE", "cli")
                .spawn()
                .unwrap(),
        );
        assert!(child.0.wait().unwrap().success());
        return;
    }
    assert_eq!(stage, "cli");
    let claim = Process::current().unwrap().identify().unwrap();
    let mut socket =
        UnixStream::connect(std::env::var_os("NEXUS_IDENTITY_FIXTURE_SOCKET").unwrap()).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&claim).unwrap();
    socket
        .write_all(&(bytes.len() as u32).to_le_bytes())
        .unwrap();
    socket.write_all(&bytes).unwrap();
    let mut acknowledged = [0];
    socket.read_exact(&mut acknowledged).unwrap();
    assert_eq!(acknowledged, [1]);
}
