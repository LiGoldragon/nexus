//! `Parser` — nexus text → [`signal::Request`].
//!
//! This is the current Criome-specific parser. It still accepts only the
//! pre-renovation Assert surface while `signal` is moved to the seven-root
//! contract. The Tier 0 target in `spec/grammar.md` is explicit root-record
//! dispatch: `(Assert ...)`, `(Match ...)`, `(Subscribe ...)`, and so on.
//!
//! Until the signal boundary is rebased onto the seven-root contract, this
//! parser remains a compatibility adapter. Retired Nexus sigils and piped
//! delimiters are not tokenized by nota-codec anymore, so this parser does not
//! preserve the old `(| ... |)` query form.
//!
//! `Request::Handshake` does not appear in user-facing text —
//! the daemon performs the handshake with criome internally
//! when opening each per-connection signal session.

use nota_codec::{Decoder, NotaDecode, Token};
use signal::{AssertOperation, Edge, Graph, Node, RelationKind, Request, Slot};

use crate::error::Result;

pub struct Parser<'input> {
    decoder: Decoder<'input>,
}

impl<'input> Parser<'input> {
    /// Open a parser over a slice of nexus text.
    pub fn new(input: &'input str) -> Self {
        Self {
            decoder: Decoder::new(input),
        }
    }

    /// Read the next top-level request, or `None` at end of
    /// input. Errors leave the decoder mid-stream — the caller
    /// stops on the first error since reply positions would
    /// otherwise lose sync with the surviving requests.
    pub fn next_request(&mut self) -> Result<Option<Request>> {
        match self.decoder.peek_token()? {
            None => Ok(None),
            Some(Token::LParen) => {
                let operation = self.next_assert_operation()?;
                Ok(Some(Request::Assert(operation)))
            }
            Some(other) => Err(nota_codec::Error::UnexpectedToken {
                expected: "`(` opening the current compatibility Assert record",
                got: other,
            }
            .into()),
        }
    }

    fn next_assert_operation(&mut self) -> Result<AssertOperation> {
        self.decoder.expect_record_start()?;
        let head = self.decoder.read_pascal_identifier()?;
        let operation = match head.as_str() {
            "Node" => AssertOperation::Node(self.next_node()?),
            "Edge" => AssertOperation::Edge(self.next_edge()?),
            "Graph" => AssertOperation::Graph(self.next_graph()?),
            other => {
                return Err(nota_codec::Error::UnknownVariant {
                    enum_name: "AssertOperation",
                    got: other.to_string(),
                }
                .into());
            }
        };
        self.decoder.expect_record_end()?;
        Ok(operation)
    }

    fn next_node(&mut self) -> Result<Node> {
        Ok(Node {
            name: String::decode(&mut self.decoder)?,
        })
    }

    fn next_edge(&mut self) -> Result<Edge> {
        Ok(Edge {
            from: Slot::<Node>::decode(&mut self.decoder)?,
            to: Slot::<Node>::decode(&mut self.decoder)?,
            kind: RelationKind::decode(&mut self.decoder)?,
        })
    }

    fn next_graph(&mut self) -> Result<Graph> {
        Ok(Graph {
            title: String::decode(&mut self.decoder)?,
            nodes: Vec::<Slot<Node>>::decode(&mut self.decoder)?,
            edges: Vec::<Slot<Edge>>::decode(&mut self.decoder)?,
            subgraphs: Vec::<Slot<Graph>>::decode(&mut self.decoder)?,
        })
    }
}
