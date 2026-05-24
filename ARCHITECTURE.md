# ARCHITECTURE — nexus

Nexus is the workspace's typed semantic text vocabulary over NOTA
syntax. This repo owns the Nexus vocabulary/spec and the translator
daemon. It does not own a second parser or a second text syntax.

> **Scope.** "criome" throughout this doc is today's `criome` daemon
> (sema-ecosystem records validator). The eventual `Criome` is the
> universal computing paradigm in Sema; at that point text↔record
> translation is one operation inside the Sema substrate and nexus's
> separate-translator role goes away. Today's nexus is a realization
> step. See `~/primary/ESSENCE.md` §"Today and eventually".

1. `spec/grammar.md` — the canonical Nexus Tier 0 vocabulary/spec:
   explicit verb records written in NOTA syntax.
2. `spec/examples/` — canonical and illustrative files showing
   explicit request records.
3. `src/` — the daemon binary `nexus-daemon` plus the one-shot
   binaries `nexus-parse` and `nexus-render`. The daemon speaks
   **NOTA text containing Nexus records** on the client side (UDS at
   `/tmp/nexus.sock`) and **Signal** (rkyv) on the criome side
   (UDS at `/tmp/criome.sock`); the one-shots are stdin/stdout
   wrappers around the same `Parser` / `Renderer` types for
   test pipelines and agent harnesses. Holds no sema state —
   purely a translator.

```
client (nexus-cli, agents, editors, shell scripts)
   │
   │ NOTA text containing Nexus records in / out
   │
   ▼
┌──────────┐
│  nexus   │   parse NOTA via nota-codec (Decoder::new)
│ (daemon) │   build Signal frames, send to criome
│          │   receive Signal replies, render to text via nota-codec (Encoder::new)
└────┬─────┘
     │
     │ Signal (rkyv envelope around per-verb typed payloads)
     │
     ▼
   criome
```

## Boundaries

Owns (`[lib]` + `[[bin]]` split):

- **The Nexus vocabulary/spec** (under `spec/`). It defines typed
  request records over NOTA syntax.
- **bin half** (`src/main.rs`): the daemon process — UDS
  listener at `/tmp/nexus.sock`, parsing, Signal connection
  to criome, reply rendering.
- **lib half** (`src/lib.rs` + `src/error.rs`): daemon-specific
  helpers — typed errors and (soon) connection-state types
  + a request-routing actor.
- The **mechanical translation rule**: every Nexus record
  construct has exactly one Signal form, and vice versa.

Does not own:

- Lexer / Decoder / Encoder kernel — lives in
  nota-codec.
  Per-kind parsing of records, typed pattern markers, verbs, and
  primitives is performed by the derives in
  nota-derive
  (`NotaRecord`, `NotaEnum`, `NotaTransparent`,
  `NotaTryTransparent`, `NotaSum`) which Signal types apply.
  `PatternField<T>`, `(Bind)`, and `(Wildcard)` live in
  signal-core as ordinary typed records over Nota syntax.
- The Signal envelope and per-verb typed IR — lives in
  signal.
- Sema state — that's criome's exclusive concern.
- The validator pipeline.

## Two messaging surfaces

The nexus daemon is the *only* place where these meet:

| Surface | Direction | Format | Contents |
|---|---|---|---|
| **client-facing** | client ↔ nexus | NOTA syntax | Nexus request records in / reply records out |
| **Signal** | nexus ↔ criome | rkyv | language IR for the six root verbs (`Assert`, `Mutate`, `Retract`, `Match`, `Subscribe`, `Validate`). Atomicity is structural — a multi-op `Request<Payload>` carrying `NonEmpty<Operation>` commits as one unit; no separate `Atomic` verb. Read-algebra (`Constrain`, `Project`, `Aggregate`, `Infer`, `Recurse`) appears inside `Match`/`Subscribe`/`Validate` payloads via `sema-engine`'s `ReadPlan`, never as a root. |

NOTA text containing Nexus records is the only non-Signal messaging
surface in the sema-ecosystem. It is transient — never persisted,
never rendered outside this daemon.

## Per-connection state

The daemon holds, per open connection:

- The negotiated protocol version (from the handshake).
- Open subscription registration (one subscription per
  connection; events stream until close).

Nothing else. No domain correlation IDs in payloads —
request/reply matching is frame-layer `ExchangeIdentifier` state
(session epoch + lane + monotonic sequence) negotiated by
`signal-core` at handshake. Domain payloads never carry transport
identifiers. Nexus's client-facing text leg stays single-flight
FIFO on each connection; the Signal leg to criome is async. No
fallback-file dispatch. No resume after disconnect (durable
work is criome-state, fetched via `Match`). No sema cache.

## Code map

```
nexus/
├── spec/
│   ├── grammar.md                — the canonical Nexus vocabulary/spec
│   └── examples/                 — canonical and illustrative examples
└── src/
    ├── lib.rs                    — re-exports + supervision-tree doc
    ├── error.rs                  — typed daemon-process errors (incl. ActorCall, ActorSpawn)
    ├── daemon.rs                 — Daemon actor: root of the supervision tree; spawns Listener
    ├── listener.rs               — Listener actor: UDS accept loop, spawns Connection per accept
    ├── connection.rs             — Connection actor: per-client text shuttle (single-message Run lifecycle)
    ├── criome_link.rs            — CriomeLink struct: post-handshake signal connection (single-owner, not an actor)
    ├── parser.rs                 — Parser struct: current Criome parser; awaiting Tier 0 verb-record rewrite
    ├── renderer.rs               — Renderer struct: signal::Reply → Tier 0 text
    ├── main.rs                   — nexus-daemon entry: env config, start Kameo root Daemon, await shutdown
    └── bin/
        ├── parse.rs              — nexus-parse: stdin text → length-prefixed Frame on stdout
        └── render.rs             — nexus-render: length-prefixed Frame on stdin → text on stdout
```

The supervision tree:

```text
Daemon (root)
  └── Listener
        ├── Connection × M  (one per accepted UDS client)
        └── ...
```

Per-actor file shape — each actor file owns a data-bearing actor type,
its typed messages, and its lifecycle in one place. Parser, Renderer, and
CriomeLink stay plain structs (single-owner, no concurrent state — they're
stateless transformers / one-call-at-a-time sessions, not components
warranting their own mailboxes). The workspace actor runtime is direct
Kameo.

## Invariants

- **Text crosses only at this boundary.** All daemon-to-daemon
  internal traffic is Signal (rkyv). No raw NOTA text reaches
  criome.
- **No state survives a request.** Per-connection state dies
  with the connection; durable state lives in criome's sema.
- **No domain correlation IDs.** Request/reply matching is
  frame-layer `ExchangeIdentifier` state owned by `signal-core`.
  Domain payloads never carry transport identifiers. The Nexus
  daemon's client-facing text leg is single-flight FIFO; the
  Signal leg to criome is async.
- **One text construct, one typed value.** The mechanical
  translation rule is the perfect-specificity
  invariant
  seen at the NOTA↔Signal boundary. Every Nexus record
  names exactly one typed shape; every typed shape has exactly
  one canonical text rendering. The daemon never instantiates
  a generic record and figures out its kind later — it parses
  text directly into the precise typed payload of the verb
  the text expresses (`Assert(Node { … })`,
  `Mutate(MutateOperation { … })`,
  `Match(GraphQuery { … })`). Failure to parse
  into a known kind is a parse-time error, not a downstream
  validation miss.

## Status

**Renovating.** The spec is being renovated to Nexus Tier 0: records,
sequences, explicit request records, and schema-driven `PatternField<T>`
decoding through ordinary `(Bind)` and `(Wildcard)` records. The renderer now
emits named `SlotBinding` records for slotted query replies. The current parser
still carries the previous Criome-specific M0 surface until `signal` is rebased
onto the six-root contract with structural multi-op atomicity. Domain-
parameterizing the daemon waits until a second concrete translator exists.

## What nexus-daemon does — and only that

The nexus daemon does **one thing**: translate between NOTA text containing
Nexus records and Signal. In both directions. Nothing else.

- NOTA text in → Signal out (parsing)
- Signal in → NOTA text out (rendering)

It does not hold sema state. It does not validate beyond
syntactic well-formedness. It does not remember anything across
requests except the per-connection handshake/subscription state
needed to keep ordered replies flowing. It does not read records,
write records, or know what a record means. Translation is the
whole job.

This bright-line scope makes the daemon usable in two distinct
roles, both reducing to the same translation primitive:

- **Text-client gateway.** Humans, agents, shell scripts, and
  LLM tools send NOTA text containing Nexus records to the daemon's
  UDS; the daemon parses to Signal, forwards to criome, and renders
  the reply back to NOTA text.
- **Rendering service.** Other Signal-speaking clients (mentci-*
  GUIs being the first) speak Signal directly to criome but
  also hold a connection to the nexus daemon and use it to
  render typed Signal payloads as NOTA text containing Nexus records
  for human display (inspector views, wire-pane frame display, hover
  labels). The same daemon, the same primitive, used as a service
  rather than a gateway.

In both roles the daemon is a translator. New clients do not
extend nexus's scope — they consume the translation primitive
from whichever side they need.

## Clients connecting directly to criome

Anything that wants to *talk to* criome speaks Signal. The nexus
daemon is one such speaker (forwarding the parsed Signal from
its text clients). Others connect directly:

- **mentci-* GUIs** — speak Signal to criome for editing and
  subscriptions; *additionally* speak Signal to nexus-daemon for
  rendering as described above.
- **direct Signal speakers** — agents written in Rust, CI
  harnesses, integration tests, any binary linking signal.

Nexus is not a required intermediary for criome access; it is
the text translation primitive that humans and rendering clients
both consume.

## Parser + renderer wire-in

Adding a new typed Nexus payload under the existing six root verbs lands
in three places:

1. The typed payload + closed-enum variant in
   signal.
2. A new arm in [`Parser`](src/parser.rs) — verb-record dispatch from the
   PascalCase verb head to the typed payload.
3. A new arm in [`Renderer`](src/renderer.rs) — typed reply →
   NOTA text, one canonical rendering per typed shape.

The mechanical-translation rule (one text construct, one typed
value) extends to every payload the daemon translates; new payloads
slot into parser/renderer per-variant dispatch without adding sigils
or grammar slots.

## Cross-cutting context

- Project-wide architecture:
  criome/ARCHITECTURE.md
- Signal (the rkyv form on the criome leg):
  signal/ARCHITECTURE.md
- nota-codec (text codec used both for parsing client requests
  and rendering replies):
  nota-codec/ARCHITECTURE.md
