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

`Situation`, `Situating` and `Situated` are the observed half of a Nexus's
own metadata. `ConfigurationState` says where a Nexus intends to listen;
`Situation` says where one actually listened and which store file it actually
opened, written once both sockets are bound and never read back as
configuration. A store is portable and its configuration is not, so a copy
opened elsewhere would bind socket paths belonging to the Nexus still running
where the original lives; `is_carried` is how the copy recognises itself.

A kind enters this crate on its second implementation, never on its first. What
a library may own is what every instance must do identically and getting wrong
would be a defect rather than a design choice; a trait with no capability and no
data-bearing implementor is a note, not a kind.

The documents for Nexus and Sema remain undesigned, so this crate defines no
document grammar.

## Subscription publishing

A Signal that answers with a stream (`Stream<State Change>` in its Ethos) is
answered from a `Publisher<State, Change>`. The Publisher holds the one current
published state; the owning actor reads it through `Subscribable::state` and
keeps no second copy. It is generic over the state and change and its fields
are its own, so it is the one hand-written type of this concern; every trait it
bears and the `Capacity` and `Reached` it speaks in are declared in
`ethos/library.ethos` and generated. Its state bears `signal::Following`,
declared in signal beside `Frame` and `Stream`, since the subscriber folds each
`Changed` into its copy by the same fold; the bound `State: Following` is the
assertion that a stream's state bears it.

- `publish(change)` takes the state out of its lock, folds the change in with
  `signal::Following::after` (no copy of the state per change), advances a position
  the Publisher owns, and sends `(position, change)` on a broadcast channel of
  the configured `Capacity`, all under the one lock. It waits for no
  subscriber and yields `Reached`, the number of open streams it was handed to.
- `open()` subscribes and reads the state and its position under that same
  lock, so the stream is handed exactly the changes after the state it opened
  on. Its frames are `Opened(state)`, then `Changed(change)` for each later
  change, in order.
- A subscriber more than `Capacity` changes behind is handed `Lagged(state)`,
  the state as it then stands, and goes on. The changes still waiting for it
  are drained under the lock the state is read under, since that state already
  holds them, so it has the whole capacity again for the changes that follow.
  After a state at position `p` the stream drops every change at or before
  `p`: none twice, none missing. No
  subscriber is dropped for being slow, and none slows another or the Nexus.
- When the Publisher is dropped each stream finishes; a subscription is one
  exchange, and the serving side ends it with the exchange's `Delivery::End`.

The position is never a wire value; the frames are `signal::Frame`.

`State` and `Change` are `Clone` (a state is sent to a subscriber at open and
at lag, a change once per subscriber) and `Send + Sync + 'static`, as the
channel and the boxed stream require; those bounds are on the Publisher's
implementations, not on the Ethos traits.
