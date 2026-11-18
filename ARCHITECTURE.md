# Architecture

The `nexus` crate owns universal Nexus types and trait-borne behavior. Domain
nouns remain in each component's authored Signal and Sema contracts. The
serving every Nexus shares is in the workspace's other crates (see "Shared
serving" below); actor and runner mechanics remain each component's.

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
  the Publisher owns, and keeps `(position, change)` among the last `Capacity`
  changes, all under the one lock; then it announces the change on a watch
  channel every open stream holds a receiver of. It waits for no
  subscriber and yields `Reached`, the number of open streams it was handed to.
- `open()` takes its receiver and reads the state and its position under that
  same lock, so the stream is handed exactly the changes after the state it opened
  on. Its frames are `Opened(state)`, then `Changed(change)` for each later
  change, in order.
- A stream wakes on an announcement and reads, under the lock, the kept change
  after its position. A subscriber more than `Capacity` changes behind finds
  that change no longer kept and is handed `Lagged(state)`, the state as it then
  stands, and goes on from its position. After a state at position `p` the
  stream is handed only changes after `p`: none twice, none missing. No
  subscriber is dropped for being slow, and none slows another or the Nexus.
- `resize(capacity)`, on the mutable Publisher, sets the capacity and yields
  `Reached`, the open streams it moved; a capacity below one is refused as
  `create` refuses it, and the capacity stays. Every open stream follows it,
  since all read the one set of kept changes: a stream whose backlog is within
  the new capacity carries it over exactly, one further behind is handed
  `Lagged` with the state as it stands when it next reads, and the position
  rule keeps what follows exact.
- When the Publisher is dropped each stream finishes after the changes still
  kept for it; a subscription is one
  exchange, and the serving side ends it with the exchange's `Delivery::End`.

The position is never a wire value; the frames are `signal::Frame`.

`State` and `Change` are `Clone` (a state is sent to a subscriber at open and
at lag, a change once per subscriber) and `Send + Sync + 'static`, as the
shared lock and the boxed stream require; those bounds are on the Publisher's
implementations, not on the Ethos traits.

## Shared serving

The serving, the sockets and the text are written once, for every Nexus, and
each Nexus hand-writes only its own bodies. A dependency set that differs by
consumer is a crate of its own:

- `nexus-exchange` is what both sides of a socket share: the socket names a
  Nexus's `ComponentName` gives, the bootstrap `Admission` a pair launcher
  hands both processes and its checked `ValidAdmission`, the blocking half of
  `signal`'s exchange layer, the translator's lane frames, and `LaneFailure`,
  the failure of an emission's lane, which every emission outcome carries as
  one variant of its own through `LaneFailing`, apart from its domain
  refusals: `Unsubmitted`, the effect not done; `Declined`, refused by the
  translator before acting; `Unanswered`, sent and no answer read. Its `datom`
  feature is the one per-consumer feature: the translator's text side may
  build it, the Nexus process never does.
- `nexus-querying` is the querying end of a connection, `QueryingConnection`,
  which a translator or a test greets and opens exchanges on.
- `nexus-serving` is the daemon side. `Listeners` binds the meta socket at
  start and the public socket once the Nexus is configured; the meta socket
  answers only the paired translator, pinned by pidfd, one blocking exchange
  at a time (`MetaServing`), and the public socket admits the translator and
  the peers the Nexus's `PeerAdmitting` names. `PublicConnection` serves each
  accepted public connection on the runtime: the greeting within the
  transport timeout, then every exchange on a task of its own, its attempt on
  the blocking pool, answered once or with each frame of a `signal::Stream`
  as one `Delivery::Answer` on its exchange until `Delivery::End`. The
  emission lane sends one external effect to the translator as a Signal value
  and reads its outcome back, a failed lane as the outcome's `LaneFailure`.
  `NexusBootstrap` is the one frame a Nexus starts with, written by the pair
  launcher: its `Place`, its `Admission`, its paired translator's
  `PeerIdentity` and the `InstanceId` of this run; `BootstrapReading` reads
  it.

A Nexus implements `MetaAnswering`, `PublicServing`, `TransportBounded`,
`PeerAdmitting` and `Named` with its own bodies. Whatever Ethos can declare
is declared in `crates/exchange/ethos/exchange.ethos` and
`crates/serving/ethos/serving.ethos`; what is hand-written is what Ethos
cannot name: sockets, locks and channels, durations, futures, generic
methods over another contract's query, and a live `signal::Stream`.
