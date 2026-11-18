//! A Nexus's sockets and its Memory are named after the Nexus.

use crate::generated::{
    ComponentName, MemoryFile, MemoryNaming, MetaNexusSocket, MetaTranslatorSocket,
    PublicNexusSocket, PublicTranslatorSocket, SocketNames, SocketNaming,
};

impl SocketNaming for ComponentName {
    /// `<name>.sock` and `<name>-meta.sock` are the Nexus's own;
    /// `<name>-translator.sock` and `<name>-meta-translator.sock` its
    /// translator's.
    fn socket_names(&self) -> SocketNames {
        let name = &self.0;
        SocketNames {
            public_nexus_socket: PublicNexusSocket(format!("{name}.sock")),
            meta_nexus_socket: MetaNexusSocket(format!("{name}-meta.sock")),
            public_translator_socket: PublicTranslatorSocket(format!("{name}-translator.sock")),
            meta_translator_socket: MetaTranslatorSocket(format!("{name}-meta-translator.sock")),
        }
    }
}

impl MemoryNaming for ComponentName {
    /// `<name>.memory`, the file a Nexus keeps its Memory in.
    fn memory_file(&self) -> MemoryFile {
        MemoryFile(format!("{}.memory", self.0))
    }
}
