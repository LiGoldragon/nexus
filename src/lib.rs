//! nexus — the text-translator daemon library half.
//!
//! Speaks **nexus text** to clients (one-shot per connection in
//! M0: read until EOF, parse, forward, render, write, close)
//! and **signal** (rkyv) to criome (a paired
//! [`CriomeLink`](crate::criome_link::CriomeLink) per
//! connection, post-handshake).
//!
//! The daemon is stateless modulo per-connection state. There
//! is no correlation-id mapping — replies pair to requests by
//! position on the connection (FIFO).
//!
//! All wire-protocol types live in
//! [signal](https://github.com/LiGoldragon/signal). The
//! parser/value-codec primitives live in
//! [nota-next](https://github.com/LiGoldragon/nota-next).
//!
//! The daemon supervision tree:
//!
//! ```text
//! Daemon (root)
//!   └── Listener
//!         ├── Connection × M  (one per accepted UDS client)
//!         └── ...
//! ```

pub mod connection;
pub mod criome_link;
pub mod daemon;
pub mod error;
pub mod listener;
pub mod parser;
pub mod renderer;

pub use criome_link::CriomeLink;
pub use daemon::Daemon;
pub use error::{Error, Result};
pub use parser::Parser;
pub use renderer::Renderer;
