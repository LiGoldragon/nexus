//! The owner's declaration that a store was moved rather than copied.
//!
//! A situation record says where a store was bound. When the store is opened
//! anywhere else, the record cannot tell which of two worlds it is in: the
//! store was duplicated and the original is still a claimant, or the store
//! was carried and is the same single claimant at a new address. Nothing
//! inside the store distinguishes them, because the distinguishing fact is
//! about the world outside it.
//!
//! So the owner says. A relocation is that declaration: one store, named
//! origin, named destination. It is honoured by exactly the move it names and
//! by nothing else — not by a later copy, not by a different store that
//! happens to carry it, and not by a second move.

use nexus::{Relocated, Relocating, Relocation};

#[test]
fn a_relocation_names_the_one_move_it_declares() {
    let declared = Relocation::declared("/state/one.sema".to_owned(), "/moved/one.sema".to_owned());
    assert_eq!(declared.origin(), "/state/one.sema");
    assert_eq!(declared.destination(), "/moved/one.sema");
}

#[test]
fn a_relocation_admits_the_move_it_names() {
    let declared = Relocation::declared("/state/one.sema".to_owned(), "/moved/one.sema".to_owned());
    assert!(
        declared.admits("/state/one.sema", "/moved/one.sema"),
        "the store records the origin and is opened at the destination: this \
         is the move that was declared"
    );
}

#[test]
fn a_relocation_refuses_a_destination_it_does_not_name() {
    let declared = Relocation::declared("/state/one.sema".to_owned(), "/moved/one.sema".to_owned());
    assert!(
        !declared.admits("/state/one.sema", "/third/one.sema"),
        "a copy taken after the declaration is at neither end of it, and a \
         declaration that admitted any destination would be a standing \
         licence rather than one move"
    );
}

#[test]
fn a_relocation_refuses_an_origin_it_does_not_name() {
    let declared = Relocation::declared("/state/one.sema".to_owned(), "/moved/one.sema".to_owned());
    assert!(
        !declared.admits("/other/one.sema", "/moved/one.sema"),
        "a declaration lifted into a store that was bound somewhere else \
         names a move that store never made"
    );
}

#[test]
fn a_relocation_to_where_the_store_already_is_declares_nothing() {
    let declared = Relocation::declared("/state/one.sema".to_owned(), "/state/one.sema".to_owned());
    assert!(
        !declared.admits("/state/one.sema", "/state/one.sema"),
        "a store opened where it records itself is not carried, so there is \
         no refusal for a declaration to lift, and one that named this move \
         could only ever be a forgery aimed at some other open"
    );
}

#[test]
fn a_relocation_is_a_portable_archive() {
    let declared = Relocation::declared("/state/one.sema".to_owned(), "/moved/one.sema".to_owned());
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&declared).expect("archive a relocation");
    let restored =
        rkyv::from_bytes::<Relocation, rkyv::rancor::Error>(&bytes).expect("restore a relocation");
    assert_eq!(restored, declared);
}
