# nexus

Nexus is the typed semantic vocabulary over NOTA syntax, plus a
translator daemon.

## What's here

- `spec/grammar.md` — the Nexus vocabulary/spec over NOTA syntax.
- `spec/examples/` — illustrative examples showing explicit Nexus
  request records.
- `src/` — the daemon implementation. Parses NOTA text containing
  Nexus records via nota-next, builds
  Signal frames, dials
  criome over UDS, serialises replies back to text.

## Architecture

See `ARCHITECTURE.md` for the daemon's role
and code map.

For the project being built: criome's
ARCHITECTURE.md.

## Wire formats

- **Client side** (UDS at `/tmp/nexus.sock`): NOTA text containing
  Nexus records in / out. The parser self-delimits on matched parens;
  replies pair to requests by **position** on the connection (FIFO).
- **criome side** (UDS at `/tmp/criome.sock`):
  Signal rkyv frames
  carrying language IR.

NOTA text containing Nexus records is the only non-Signal messaging
surface in the sema-ecosystem. Once a request crosses the daemon, it
is Signal end-to-end.

## License

[License of Non-Authority](LICENSE.md).
