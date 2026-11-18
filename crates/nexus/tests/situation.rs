//! Where a Nexus actually ran, and what it makes of finding itself elsewhere.
//!
//! The persisted configuration says where a Nexus intends to listen. This
//! says where one actually listened, and which store file it actually opened
//! — the file, by the identity the filesystem gives it, and not merely the
//! name it was reached by. It is written, never read as configuration, so the
//! only question it answers is what to make of the store now being opened.
//!
//! A path is an address; a file is an identity. Conflating them makes a move
//! indistinguishable from a copy, because both change the address. Keeping
//! them apart separates the three cases a record can be in: the same file at
//! the address it records, the same file at a new address, and a different
//! file bearing its record.

use std::os::unix::fs::MetadataExt;

use nexus::{Bearing, Identifying, Situated, Situating, Situation, StoreIdentity};

/// The identity of a store that is really there, which is the only kind a
/// Nexus ever records: the guard runs after the engine has opened the file.
trait Existing {
    fn real(&self, name: &str) -> StoreIdentity;
}

impl Existing for std::path::Path {
    fn real(&self, name: &str) -> StoreIdentity {
        let path = self.join(name);
        std::fs::write(&path, b"store").expect("create a store file to identify");
        StoreIdentity::of(&path)
    }
}

#[test]
fn a_store_identity_names_the_file_the_filesystem_named() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let path = directory.path().join("one.sema");
    std::fs::write(&path, b"store").expect("create a store file");
    let identity = StoreIdentity::of(&path);
    let stat = std::fs::metadata(&path).expect("stat the store file");

    assert_eq!(identity.path(), path.display().to_string());
    assert_eq!(identity.device(), stat.dev());
    assert_eq!(identity.inode(), stat.ino());
    assert!(identity.is_named(), "a file that exists has an identity");
}

#[test]
fn a_path_with_no_file_at_it_has_an_address_but_no_identity() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let identity = StoreIdentity::of(&directory.path().join("absent.sema"));
    assert!(
        !identity.is_named(),
        "there is no file for the filesystem to name"
    );
    assert!(
        !identity.is_same_file(&StoreIdentity::of(
            &directory.path().join("also-absent.sema")
        )),
        "and two unnamed identities are not the same file: an absence is not \
         a thing two paths can share"
    );
}

#[test]
fn a_hard_link_is_the_same_file_under_a_second_address() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let first = directory.path().real("one.sema");
    let second_path = directory.path().join("also-one.sema");
    std::fs::hard_link(first.path(), &second_path).expect("a second name for one file");
    let second = StoreIdentity::of(&second_path);

    assert_ne!(first.path(), second.path());
    assert!(
        first.is_same_file(&second),
        "the address differs and the file does not"
    );
}

#[test]
fn a_copy_is_a_different_file_however_alike_its_contents() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let first = directory.path().real("one.sema");
    let copy_path = directory.path().join("copy.sema");
    std::fs::copy(first.path(), &copy_path).expect("copy the store the way a backup would");

    assert!(
        !first.is_same_file(&StoreIdentity::of(&copy_path)),
        "identical bytes are not an identity; a copy is a second claimant"
    );
}

#[test]
fn a_situation_records_the_store_file_and_the_sockets_that_were_actually_bound() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let store = directory.path().real("orchestrate-nexus.sema");
    let situation = Situation::situated(
        store.clone(),
        vec![
            "/run/orchestrate.sock".to_owned(),
            "/run/orchestrate-meta.sock".to_owned(),
        ],
    );

    assert_eq!(situation.store().path(), store.path());
    assert_eq!(situation.store().inode(), store.inode());
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
fn a_store_opened_where_its_own_record_says_it_lives_is_settled() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let store = directory.path().real("one.sema");
    let situation = Situation::situated(store.clone(), Vec::new());
    assert_eq!(situation.bearing(&store), Bearing::Settled);
}

#[test]
fn a_store_restored_in_place_is_settled_though_it_is_a_different_file() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let store = directory.path().real("one.sema");
    let situation = Situation::situated(store.clone(), Vec::new());

    std::fs::remove_file(store.path()).expect("remove the store");
    let restored = directory.path().real("one.sema");
    assert!(
        !store.is_same_file(&restored),
        "a new file at the old address"
    );
    assert_eq!(
        situation.bearing(&restored),
        Bearing::Settled,
        "a restore in place is an ordinary operation: the sockets in this \
         record belong to whoever is at this address, and this is that \
         address, so there is no second claimant to protect anyone from"
    );
}

#[test]
fn the_same_file_at_a_new_address_has_moved_and_needs_nobody_to_say_so() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let store = directory.path().real("one.sema");
    let situation = Situation::situated(store.clone(), Vec::new());

    let moved_path = directory.path().join("moved/one.sema");
    std::fs::create_dir_all(moved_path.parent().expect("a parent")).expect("create the directory");
    std::fs::rename(store.path(), &moved_path).expect("move the store");
    let moved = StoreIdentity::of(&moved_path);

    assert!(store.is_same_file(&moved));
    assert_eq!(
        situation.bearing(&moved),
        Bearing::Moved,
        "one file cannot be two claimants, so the address it answers at is \
         the owner's to change and there is nothing left to declare"
    );
}

#[test]
fn a_different_file_at_a_new_address_has_been_carried() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let store = directory.path().real("one.sema");
    let situation = Situation::situated(store.clone(), Vec::new());

    let copy_path = directory.path().join("copy.sema");
    std::fs::copy(store.path(), &copy_path).expect("copy the store");

    assert_eq!(
        situation.bearing(&StoreIdentity::of(&copy_path)),
        Bearing::Carried,
        "two files now bear one record, and binding the socket paths it \
         carries would take them from the Nexus still running at the \
         original"
    );
}

#[test]
fn a_cross_filesystem_move_is_indistinguishable_from_a_copy_and_is_carried() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let store = directory.path().real("one.sema");
    let situation = Situation::situated(store.clone(), Vec::new());

    // What `mv` does across a filesystem boundary, and what `tar`, `rsync`
    // and every snapshot restore do everywhere: the bytes arrive in a new
    // file and the old one goes away. The record cannot tell this from a
    // duplication, which is exactly why a declaration has to exist.
    let moved_path = directory.path().join("moved.sema");
    std::fs::copy(store.path(), &moved_path).expect("copy the bytes across");
    std::fs::remove_file(store.path()).expect("and remove the original");

    assert_eq!(
        situation.bearing(&StoreIdentity::of(&moved_path)),
        Bearing::Carried,
        "the origin being gone is evidence and not consent: an `rm` and the \
         second half of an `mv` leave the same absence behind"
    );
}

#[test]
fn a_situation_is_a_portable_archive() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let situation = Situation::situated(
        directory.path().real("one.sema"),
        vec!["/a.sock".to_owned()],
    );
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&situation).expect("archive a situation");
    let restored =
        rkyv::from_bytes::<Situation, rkyv::rancor::Error>(&bytes).expect("restore a situation");
    assert_eq!(restored, situation);
}
