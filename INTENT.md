# INTENT — nexus

*What the psyche has explicitly intended for this project. Synthesised
from psyche statements and the applicable workspace constraints; not
embellished. Maintenance: `primary/skills/repo-intent.md`.*

`nexus` is the workspace's typed semantic text vocabulary written in
NOTA syntax, plus a translator daemon. It owns the Nexus
vocabulary/spec (explicit verb records written in NOTA) and the
`nexus-daemon` that translates between NOTA text containing Nexus
records and Signal frames. It is not a second parser and not a second
text syntax.

## Repo-scope only

This file carries vocabulary-and-translator intent for `nexus`. The
NOTA codec kernel is `nota-codec`; the per-kind derives are
`nota-derive`; the Signal envelope and typed IR live in the Signal
contract crates; Sema state belongs to the consuming component
(criome today). Workspace-shape intent stays in `primary/INTENT.md`.

## Goals

- Define typed Nexus request records over NOTA syntax — the typed
  semantic content layer — and keep NOTA as the only text syntax with
  Nexus as content written in it, never an alternate format.
- Provide a daemon whose single job is mechanical translation between
  NOTA text containing Nexus records and Signal, usable both as a
  text-client gateway and as a rendering service for Signal-speaking
  clients.

## Constraints

- **Text crosses only at this boundary.** All daemon-to-daemon internal
  traffic is Signal (rkyv); no raw NOTA text reaches the downstream
  component. NOTA text containing Nexus records is the only non-Signal
  messaging surface, and it is transient — never persisted, never
  rendered outside this daemon.
- **One text construct, one typed value.** The mechanical-translation
  rule is perfect specificity at the NOTA↔Signal boundary: every Nexus
  record names exactly one typed shape, and every typed shape has
  exactly one canonical text rendering. The daemon parses text directly
  into the precise typed payload; failure to parse into a known kind is
  a parse-time error, not a downstream validation miss.
- **No state survives a request.** Per-connection state (negotiated
  protocol version, one open subscription) dies with the connection;
  durable state lives downstream and is fetched via `Match`. No
  fallback-file dispatch, no resume after disconnect, no sema cache.
- **No domain correlation identifiers.** Request/reply matching stays
  in the frame/session layer; domain payloads never carry transport
  identifiers.
- **NOTA strings come from bracket forms.** The text surface uses
  `[text]` / `[|text|]` bracket strings and typed pattern records
  (`(Bind)`, `(Wildcard)`); retired sigils and piped delimiters are not
  part of the surface.

## Today and eventually

Today's `nexus` is a separate translator and a realization step. When
the workspace self-hosts on the eventual `Sema` substrate, text↔record
translation becomes one operation inside Sema and the separate-translator
role goes away.

## See also

- `ARCHITECTURE.md` — the two messaging surfaces, supervision tree,
  invariants, the parse/render wire-in, and translator scope.
- `../nexus-cli/INTENT.md` — the thin reference text client.
- `../nota-codec/ARCHITECTURE.md` — the NOTA codec kernel this consumes.
- `primary/skills/nota-design.md` — bracket-string discipline.
