//! Where a Nexus actually ran, and the copy it refuses to be.
//!
//! The persisted configuration says where a Nexus intends to listen. This
//! says where one actually listened, and which store file it actually opened.
//! It is written, never read as configuration — so the only question it has
//! to answer is whether the store now being opened is the one it was written
//! into, or a copy of it that has been carried somewhere else.

use nexus::{Situated, Situating, Situation};

#[test]
fn a_situation_records_the_store_and_the_sockets_that_were_actually_bound() {
    let situation = Situation::situated(
        "/state/orchestrate-nexus.sema".to_owned(),
        vec![
            "/run/orchestrate.sock".to_owned(),
            "/run/orchestrate-meta.sock".to_owned(),
        ],
    );
    assert_eq!(situation.store_path(), "/state/orchestrate-nexus.sema");
    assert_eq!(
        situation.bound_socket_vector(),
        ["/run/orchestrate.sock", "/run/orchestrate-meta.sock"]
    );
    assert_eq!(
        situation.process_id(),
        i64::from(std::process::id()),
        "the record names the process that bound those sockets"
    );
    assert!(
        !situation.boot_identity().is_empty(),
        "and the boot it bound them in, so a record surviving a reboot is \
         distinguishable from one written by a process still running"
    );
}

#[test]
fn a_store_opened_where_its_own_record_says_it_lives_is_not_a_copy() {
    let situation = Situation::situated("/state/one.sema".to_owned(), Vec::new());
    assert!(!situation.is_carried("/state/one.sema"));
}

#[test]
fn a_store_opened_anywhere_else_is_a_copy_and_says_where_it_came_from() {
    let situation = Situation::situated("/state/one.sema".to_owned(), Vec::new());
    assert!(
        situation.is_carried("/elsewhere/one.sema"),
        "a store carried to a second path would otherwise bind the first \
         path's sockets, which belong to the Nexus still running there"
    );
    assert_eq!(situation.store_path(), "/state/one.sema");
}

#[test]
fn a_situation_is_a_portable_archive() {
    let situation = Situation::situated("/state/one.sema".to_owned(), vec!["/a.sock".to_owned()]);
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&situation).expect("archive a situation");
    let restored =
        rkyv::from_bytes::<Situation, rkyv::rancor::Error>(&bytes).expect("restore a situation");
    assert_eq!(restored, situation);
}
