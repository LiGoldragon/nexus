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

The documents for Nexus and Sema remain undesigned, so this crate defines no
document grammar.
