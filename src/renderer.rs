//! `Renderer` — [`signal::Reply`] → nexus text.
//!
//! Per-position dispatch on the reply variant; each reply
//! position produces one top-level text expression. Successive
//! replies are separated by `\n` so the client sees one
//! self-delimited expression per line.
//!
//! Per signal/ARCH the reply types are wire-only (no uniform
//! `NotaEncode` derive); rendering is application-specific to
//! the daemon. Data-record rendering uses explicit Nexus heads
//! (`Node`, `Edge`, `Graph`), while records returned from Sema
//! are wrapped in an explicit `SlotBinding` record so the text
//! boundary does not expose anonymous tuples.
//!
//! `Reply::HandshakeAccepted` / `HandshakeRejected` should
//! never reach the renderer in normal operation — the daemon
//! consumes them on the criome connection during its own
//! handshake. Encountering one in the user-visible reply
//! stream is a daemon protocol error.

use nota_next::{Delimiter, NotaEncode};
use signal::{Diagnostic, Edge, Graph, Node, OutcomeMessage, Records, Reply, Slot};

use crate::error::{Error, Result};

pub struct Renderer {
    output: String,
}

impl Default for Renderer {
    fn default() -> Self {
        Self::new()
    }
}

impl Renderer {
    pub fn new() -> Self {
        Self {
            output: String::new(),
        }
    }

    /// Append the rendered text of `reply` to the buffer.
    /// Inserts a `\n` separator before non-first replies.
    pub fn render_reply(&mut self, reply: &Reply) -> Result<()> {
        let rendered = Self::render_into(reply)?;
        if !self.output.is_empty() {
            self.output.push('\n');
        }
        self.output.push_str(&rendered);
        Ok(())
    }

    /// Consume the renderer and return the accumulated text.
    pub fn into_text(self) -> String {
        self.output
    }

    fn render_into(reply: &Reply) -> Result<String> {
        match reply {
            Reply::Outcome(outcome) => Ok(Self::render_outcome(outcome)),
            Reply::Outcomes(outcomes) => {
                Ok(Delimiter::SquareBracket.wrap(outcomes.iter().map(Self::render_outcome)))
            }
            Reply::Records(records) => Ok(Self::render_records(records)),
            Reply::HandshakeAccepted(_) => Err(Error::HandshakePostReplyShape {
                got: "HandshakeAccepted",
            }),
            Reply::HandshakeRejected(_) => Err(Error::HandshakePostReplyShape {
                got: "HandshakeRejected",
            }),
        }
    }

    fn render_outcome(outcome: &OutcomeMessage) -> String {
        match outcome {
            OutcomeMessage::Ok(_) => Self::render_ok(),
            OutcomeMessage::Diagnostic(diagnostic) => Self::render_diagnostic(diagnostic),
        }
    }

    fn render_ok() -> String {
        Delimiter::Parenthesis.wrap(["Ok".to_owned()])
    }

    /// `(Diagnostic <Level> <code> <message>)`. The full
    /// Diagnostic shape (primary_site / context / suggestions /
    /// durable_record) is omitted in M0; richer rendering lands
    /// when those fields actually carry information.
    fn render_diagnostic(diagnostic: &Diagnostic) -> String {
        Delimiter::Parenthesis.wrap([
            "Diagnostic".to_owned(),
            diagnostic.level.to_nota(),
            diagnostic.code.to_nota(),
            diagnostic.message.to_nota(),
        ])
    }

    fn render_records(records: &Records) -> String {
        match records {
            Records::Node(items) => Self::render_node_bindings(items),
            Records::Edge(items) => Self::render_edge_bindings(items),
            Records::Graph(items) => Self::render_graph_bindings(items),
        }
    }

    fn render_node_bindings(items: &[(Slot<Node>, Node)]) -> String {
        Delimiter::SquareBracket.wrap(
            items
                .iter()
                .map(|(slot, value)| Self::render_node_binding(slot, value)),
        )
    }

    fn render_edge_bindings(items: &[(Slot<Edge>, Edge)]) -> String {
        Delimiter::SquareBracket.wrap(
            items
                .iter()
                .map(|(slot, value)| Self::render_edge_binding(slot, value)),
        )
    }

    fn render_graph_bindings(items: &[(Slot<Graph>, Graph)]) -> String {
        Delimiter::SquareBracket.wrap(
            items
                .iter()
                .map(|(slot, value)| Self::render_graph_binding(slot, value)),
        )
    }

    fn render_node_binding(slot: &Slot<Node>, value: &Node) -> String {
        Delimiter::Parenthesis.wrap([
            "SlotBinding".to_owned(),
            slot.to_nota(),
            Self::render_node(value),
        ])
    }

    fn render_edge_binding(slot: &Slot<Edge>, value: &Edge) -> String {
        Delimiter::Parenthesis.wrap([
            "SlotBinding".to_owned(),
            slot.to_nota(),
            Self::render_edge(value),
        ])
    }

    fn render_graph_binding(slot: &Slot<Graph>, value: &Graph) -> String {
        Delimiter::Parenthesis.wrap([
            "SlotBinding".to_owned(),
            slot.to_nota(),
            Self::render_graph(value),
        ])
    }

    fn render_node(value: &Node) -> String {
        Delimiter::Parenthesis.wrap(["Node".to_owned(), value.name.to_nota()])
    }

    fn render_edge(value: &Edge) -> String {
        Delimiter::Parenthesis.wrap([
            "Edge".to_owned(),
            value.from.to_nota(),
            value.to.to_nota(),
            value.kind.to_nota(),
        ])
    }

    fn render_graph(value: &Graph) -> String {
        Delimiter::Parenthesis.wrap([
            "Graph".to_owned(),
            value.title.to_nota(),
            value.nodes.to_nota(),
            value.edges.to_nota(),
            value.subgraphs.to_nota(),
        ])
    }

    /// Render a daemon-side error (parser failure, internal
    /// error) directly as a `(Diagnostic …)` text reply. Used
    /// when the parser rejects user text before the request
    /// can reach criome.
    pub fn render_local_error(&mut self, error: &Error) -> Result<()> {
        let rendered = Delimiter::Parenthesis.wrap([
            "Diagnostic".to_owned(),
            "Error".to_owned(),
            Self::local_error_code(error).to_owned().to_nota(),
            error.to_string().to_nota(),
        ]);
        if !self.output.is_empty() {
            self.output.push('\n');
        }
        self.output.push_str(&rendered);
        Ok(())
    }

    fn local_error_code(error: &Error) -> &'static str {
        match error {
            Error::Codec(_) => "E0001",
            Error::VerbNotInM0Scope { .. } => "E0099",
            Error::Io(_) => "E0010",
            Error::Frame(_) => "E0011",
            Error::FrameTooLarge { .. } => "E0012",
            Error::HandshakeRejected { .. } => "E0020",
            Error::HandshakePostReplyShape { .. } => "E0021",
            Error::ActorCall(_) => "E0030",
            Error::ActorSpawn(_) => "E0031",
        }
    }
}
