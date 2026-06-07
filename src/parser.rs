//! `Parser` — nexus text → [`signal::Request`].
//!
//! This is the current Criome-specific parser. It still accepts only the
//! pre-renovation Assert surface while `signal` is moved to the seven-root
//! contract. The Tier 0 target in `spec/grammar.md` is explicit root-record
//! dispatch: `(Assert ...)`, `(Match ...)`, `(Subscribe ...)`, and so on.
//!
//! Until the signal boundary is rebased onto the seven-root contract, this
//! parser remains a compatibility adapter. Retired Nexus sigils and piped
//! delimiters are not part of the `nota-next` value surface, so this parser
//! does not preserve the old `(| ... |)` query form.
//!
//! `Request::Handshake` does not appear in user-facing text —
//! the daemon performs the handshake with criome internally
//! when opening each per-connection signal session.

use std::marker::PhantomData;

use nota_next::{Block, Delimiter, Document, NotaBlock, NotaDecode, NotaDecodeError};
use signal::{AssertOperation, Edge, Graph, Node, RelationKind, Request, Slot};

use crate::error::Result;

pub struct Parser<'input> {
    root_objects: Vec<Block>,
    next_index: usize,
    parse_error: Option<NotaDecodeError>,
    source_lifetime: PhantomData<&'input str>,
}

impl<'input> Parser<'input> {
    /// Open a parser over a slice of nexus text.
    pub fn new(input: &'input str) -> Self {
        match Document::parse(input) {
            Ok(document) => Self {
                root_objects: document.root_objects().to_vec(),
                next_index: 0,
                parse_error: None,
                source_lifetime: PhantomData,
            },
            Err(error) => Self {
                root_objects: Vec::new(),
                next_index: 0,
                parse_error: Some(error.into()),
                source_lifetime: PhantomData,
            },
        }
    }

    /// Read the next top-level request, or `None` at end of
    /// input. The caller stops on the first error since reply
    /// positions would otherwise lose sync with the surviving
    /// requests.
    pub fn next_request(&mut self) -> Result<Option<Request>> {
        if let Some(error) = self.parse_error.take() {
            return Err(error.into());
        }
        let Some(root) = self.root_objects.get(self.next_index) else {
            return Ok(None);
        };
        self.next_index += 1;
        let operation = Self::assert_operation_from_block(root)?;
        Ok(Some(Request::Assert(operation)))
    }

    fn assert_operation_from_block(block: &Block) -> Result<AssertOperation> {
        let children = NotaBlock::new(block).expect_delimited(
            Delimiter::Parenthesis,
            "current compatibility Assert record",
        )?;
        let Some((head, payload)) = children.split_first() else {
            return Err(NotaDecodeError::ExpectedRootCount {
                type_name: "AssertOperation",
                expected: 1,
                found: 0,
            }
            .into());
        };
        let head = head
            .demote_to_string()
            .ok_or(NotaDecodeError::ExpectedAtom {
                type_name: "AssertOperation head",
            })?;
        match head {
            "Node" => Ok(AssertOperation::Node(Node::from_body_objects(payload)?)),
            "Edge" => Ok(AssertOperation::Edge(Self::edge_from_body(payload)?)),
            "Graph" => Ok(AssertOperation::Graph(Self::graph_from_body(payload)?)),
            other => Err(NotaDecodeError::UnknownVariant {
                enum_name: "AssertOperation",
                variant: other.to_owned(),
            }
            .into()),
        }
    }

    fn edge_from_body(payload: &[Block]) -> Result<Edge> {
        if payload.len() != 3 {
            return Err(NotaDecodeError::ExpectedRootCount {
                type_name: "Edge",
                expected: 3,
                found: payload.len(),
            }
            .into());
        }
        Ok(Edge {
            from: Slot::<Node>::from_nota_block(&payload[0])?,
            to: Slot::<Node>::from_nota_block(&payload[1])?,
            kind: RelationKind::from_nota_block(&payload[2])?,
        })
    }

    fn graph_from_body(payload: &[Block]) -> Result<Graph> {
        Graph::from_body_objects(payload).map_err(Into::into)
    }
}
