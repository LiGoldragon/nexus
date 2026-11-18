# Upgrades

## nexus-exchange, nexus-querying and nexus-serving 0.1.1 — the socket paths are the exchange's

`SocketPaths<Component>` (`From<&Place>` for a `Named` component: `public`,
`meta`, `translator`, `meta_translator`) moves from `nexus-serving` into
`nexus-exchange`, unchanged, so a client end takes a translator's socket path
from it without linking the serving's tokio. It is hand-written: a declared
Ethos type takes no type parameter and archives no path. `nexus-serving`
re-exports it as `nexus_serving::SocketPaths`, so a consumer naming it there
changes nothing. `nexus-querying` changes no code; it moves with
`nexus-exchange`, so a consumer locks one revision of the three. `nexus`
stays 0.9.0 at `d020a62`.

To adopt: repin the three crates to this repository's revision, together, and
take the build base that registers them.

## nexus-exchange, nexus-querying and nexus-serving 0.1.0 — the serving every Nexus shares

The repository is a workspace of four crates, each with its own version, on
the build base (`flake.nix` takes `github:LiGoldragon/build-base`; `Cargo.nix`
and `crate-hashes.json` are committed). `nexus` itself moves to
`crates/nexus` and is unchanged at 0.9.0; a consumer pinning it by revision
keeps `d020a62`.

The serving, the sockets and the text are written once, for every Nexus; a
Nexus hand-writes only its own bodies. Three crates are added, each a
dependency set of its own, so the Nexus process links no `datom-codec` and no
`protos`:

- `nexus-exchange`, what both sides of a Nexus socket share. Declared in
  `crates/exchange/ethos/exchange.ethos`: `ComponentName`, `ComponentTitle`,
  the four socket names and `SocketNames`, `Lane`, `Kind`, `Admission` and its
  three integers, and the traits `Named` (`name`, `title`) and `SocketNaming`
  (`ComponentName` names `<name>.sock`, `<name>-meta.sock`,
  `<name>-translator.sock`, `<name>-meta-translator.sock`). Hand-written:
  `ValidAdmission` (`TryFrom<&Admission>`), `ExchangeError`, and the blocking
  exchange layer as traits, `EndingFrameReading::read_frame_or_end`,
  `IdleFrameReading::read_frame_after_idle`, `ValueRestoring::restored`,
  `ValueWriting::write_value`, `RetryingAccept::accept_with_retry`; the
  `lane` module, the translator's lane frames (`Frame`, `TransportError`,
  `IncompleteSubmission`, `HEADER_BYTES`, `LaneReading`, `IdleLaneReading`,
  `LaneWriting`, `PeerCalling`, `Textual`). Its `datom` feature is the one
  per-consumer feature; the build base holds it as the named sets `plain` and
  `datom`.
- `nexus-querying`, the client end: `Patience` and `QueryingConnection<Q, R>`
  bearing `Querying` (`greeted`, `open`, `delivery`).
- `nexus-serving`, the daemon side. Declared in
  `crates/serving/ethos/serving.ethos`: `Closing`, `Continuation`,
  `EmissionFailure` and the trait `MetaAnswering` (`answer`). Hand-written:
  `SocketPaths<Component>` (`From<&Place>` for a `Named` component),
  `OwnedListener` (`SocketBinding`), `PairedPeer` (`PeerMatching`:
  `capture`, `matches`, `require_peer`), `Listeners<Component>`
  (`Listening`: `bind` binds the meta socket alone, `stage_public`,
  `publish_public`, `serve_next_meta`, `accept_public`) with
  `AcceptedPublic<Authority>` and `PeerAdmitting`, `MetaServing` (the blocking
  meta exchange loop), `PublicConnection<Nexus>` (`ConnectionServing::serve`)
  for a Nexus bearing `PublicServing` and `TransportBounded`, `Reply`,
  `SubscriptionClosing` (`Closable`), `AsyncFrameReading` and
  `AsyncValueWriting`, and `EmissionLane` (`Emitting::emit`).

What a Nexus writes: `MetaAnswering::answer` for a meta query;
`PublicServing` for its public socket (`configuration`, `admitted`, `answer`
on the blocking pool, `carried` for a subscription, `answered`,
`subscribed`); `PeerAdmitting` for the peers it binds; `Named` for its name.
The wire is Capsule Nexus's as it was: the exchange layer's frames, the lane
header and the emission lane are unchanged. A diagnostic names the Nexus by
its `ComponentTitle`; `ExchangeError` and `TransportError` display as
`Nexus exchange error` and `Nexus transport error`.

Pins: signal 11.2.0 (`4f5cbb9`), translator 0.5.2 (`29601a3`, feature
`unix`), tokio 1.53.1, futures-util 0.3.34, rkyv 0.8; ethos-zero 30.0.1
(`2999292`) generates and checks both Ethos files. To adopt: depend on the
three crates at one revision of this repository, and register them in the
build base.

## 0.9.0 — Publishing sets the capacity

`Publishing` gains `resize!{ [ Capacity ] [ Result<Reached CapacityRefusal> ] }`,
generated as `fn resize(&mut self, input: Capacity) -> Result<Reached,
CapacityRefusal>`. It sets the capacity of a Publisher with open streams and
yields `Reached`, the streams it moved; a capacity below one is refused as
`create` refuses it, and the capacity stays. Every open stream follows the new
capacity: one whose backlog fits is carried over exactly, one further behind is
handed `Lagged` with the state as it stands and goes on, exact after it.

The Publisher now keeps the last `Capacity` changes itself, under the lock its
state is held under, and wakes its streams on a `tokio::sync::watch`
announcement, in place of a broadcast channel. The frames a stream yields are
unchanged. To deploy: repin `nexus`; an implementor of `Publishing` other than
`Publisher` adds `resize`.

## 0.8.0 — Following moves to signal

`nexus::Following` is removed; the same trait is `signal::Following`, declared
in signal 11.2.0 (4f5cbb9), which nexus now pins. The Publisher bears the same
bounds, `State: Following<Change = Change>`, with signal's trait. To deploy:
repin `signal` to 4f5cbb9 with `nexus` and import `Following` from `signal` in
place of `nexus`. Behaviour is unchanged.

## 0.7.0 — a capacity below one is refused, not a panic

`Publisher::new` is gone. A Publisher is made by `Publishing::create(state,
Capacity)`, which answers `Result<Publisher, CapacityRefusal>`:
`CapacityRefusal.[ Zero Negative ]` is declared in `ethos/library.ethos`. To
deploy: replace `Publisher::new(state, capacity)` with
`Publisher::<State, Change>::create(state, capacity)` and handle the refusal.

## 0.6.0 — the subscription Publisher; nexus takes Ethos

Added: `Publisher<State, Change>`, the one current published state of a
subscription and the streams that follow it, built with
`Publisher::new(state, Capacity)`; it implements `Publishing` (`publish`,
`open`) and `Subscribable` (`state`) for any `State: Following<Change =
Change>`. Added with it, declared in `ethos/library.ethos` and generated by
ethos-zero 30.0.0 (b97d952): `Capacity`, `Reached`, and the traits
`Subscribable`, `Following` and `Publishing`.

nexus now has a build script, which asserts the committed
`src/generated/library.rs` is what ethos-zero generates from
`ethos/library.ethos`, and a `datom` feature (datom-codec 09a0526, protos
c93b598) under which the generated kinds derive the Datom codec's traits.

New dependencies: `signal` 11.1.0 at b145ddb, for `signal::Frame` and
`signal::Stream`; `tokio` 1.53.1 (`sync`);
`futures-util` 0.3.34. The minimum Rust is 1.89, as signal and ethos-zero
require.

Nothing breaks: every existing type is unchanged. A consumer that answers a
subscription repins `nexus` and `signal` together, so one signal sits in its
graph.
