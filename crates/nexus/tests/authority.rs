//! The access a Nexus socket grants, and what it does with a peer.
//!
//! Every Nexus opens at least two sockets: the ordinary one, for any
//! authenticated peer, and the meta socket, which is the Nexus's root. The
//! two differ in exactly two observable ways — the file mode they are bound
//! with, and which peers they answer — so those two are what this exercises.

use nexus::{Permissive, SocketAuthority};

#[test]
fn the_privileged_authority_admits_its_owner_and_nobody_else() {
    let owner = 1001;
    assert!(SocketAuthority::Privileged.admits(owner, owner));
    for peer in [0, 1, 1000, 1002, u32::MAX] {
        assert!(
            !SocketAuthority::Privileged.admits(peer, owner),
            "user {peer} is not the Nexus's own user {owner}, root included"
        );
        assert!(
            SocketAuthority::Ordinary.admits(peer, owner),
            "the ordinary socket admits whoever the filesystem let through"
        );
    }
}

#[test]
fn the_privileged_socket_mode_grants_nothing_beyond_its_owner() {
    assert_eq!(SocketAuthority::Privileged.mode() & 0o077, 0);
    assert_eq!(SocketAuthority::Privileged.mode(), 0o600);
    assert_eq!(SocketAuthority::Ordinary.mode(), 0o660);
    assert_eq!(
        SocketAuthority::Ordinary.mode() & 0o007,
        0,
        "neither socket is world-reachable"
    );
}
