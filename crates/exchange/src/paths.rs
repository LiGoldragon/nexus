//! A Nexus's four socket paths, in its place's runtime directory.
//!
//! The paths are hand-written: a declared Ethos type takes no type parameter
//! and archives no path, and `SocketPaths` is generic over the Nexus it names.

use std::marker::PhantomData;
use std::path::PathBuf;

use crate::generated::{Named, SocketNaming};
use shared_translator::placement::Place;

/// The paths of the Nexus `Component`'s sockets and its translator's.
pub struct SocketPaths<Component> {
    pub public: PathBuf,
    pub meta: PathBuf,
    pub translator: PathBuf,
    pub meta_translator: PathBuf,
    component: PhantomData<fn() -> Component>,
}

impl<Component: Named> From<&Place> for SocketPaths<Component> {
    fn from(place: &Place) -> Self {
        let directory = shared_translator::placement::runtime_path(place);
        let names = Component::name().socket_names();
        Self {
            public: directory.join(names.public_nexus_socket.0),
            meta: directory.join(names.meta_nexus_socket.0),
            translator: directory.join(names.public_translator_socket.0),
            meta_translator: directory.join(names.meta_translator_socket.0),
            component: PhantomData,
        }
    }
}

impl<Component> Clone for SocketPaths<Component> {
    fn clone(&self) -> Self {
        Self {
            public: self.public.clone(),
            meta: self.meta.clone(),
            translator: self.translator.clone(),
            meta_translator: self.meta_translator.clone(),
            component: PhantomData,
        }
    }
}

impl<Component> std::fmt::Debug for SocketPaths<Component> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SocketPaths")
            .field("public", &self.public)
            .field("meta", &self.meta)
            .field("translator", &self.translator)
            .field("meta_translator", &self.meta_translator)
            .finish()
    }
}

impl<Component> PartialEq for SocketPaths<Component> {
    fn eq(&self, other: &Self) -> bool {
        self.public == other.public
            && self.meta == other.meta
            && self.translator == other.translator
            && self.meta_translator == other.meta_translator
    }
}

impl<Component> Eq for SocketPaths<Component> {}
