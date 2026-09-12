# Architecture

This crate owns universal Nexus types and trait-borne behavior. Domain nouns
remain in each component's authored Signal and Sema contracts. Transport,
listener, actor, and runner mechanics remain in runtime libraries.

`ConfigurationState<Configuration>` is persisted by a component in the same
Sema boundary as its typed configuration. `Configurable<Configuration>` names
the permitted transitions. Ordinary Configure does not set the historical
meta marker; only a successful meta Configure does. Only a meta operation can
reverse that marker.

`desired_configuration` is the persisted configuration. The active process
uses the snapshot loaded at startup; settings that determine bound resources
take effect on restart. The stable Sema discovery path is an executable-owned
default and is never part of mutable configuration. This prevents a Configure
request from redirecting the next start to an empty database and losing the
component's existing domain state.

`SocketAuthority` and `Permissive` name the access one socket grants: the file
mode it is bound with, and which peers it answers once the mode has let them
connect. Both halves are universal — every Nexus opens an ordinary socket and a
privileged meta socket, and binding the privileged one the way the ordinary one
is bound leaves the Nexus with no privileged surface at all. What a refused peer
is told is not universal: that is a value of the contract the socket bears, and
stays with the component.

A kind enters this crate on its second implementation, never on its first. What
a library may own is what every instance must do identically and getting wrong
would be a defect rather than a design choice; a trait with no capability and no
data-bearing implementor is a note, not a kind.

The documents for Nexus and Sema remain undesigned, so this crate defines no
document grammar.
