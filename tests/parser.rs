//! Parser tests — text → `signal::Request`.
//!
//! These exercise the public Parser API end-to-end. The
//! shuttle (UDS / criome connection) is tested separately;
//! these tests stay in-process and assert on the parsed
//! `Request` shape.

use nexus::Parser;
use signal::{AssertOperation, Edge, Node, RelationKind, Request, Slot};

#[test]
fn empty_input_returns_none() {
    let mut parser = Parser::new("");
    let result = parser.next_request().expect("empty parse must succeed");
    assert!(result.is_none(), "empty input must yield None");
}

#[test]
fn whitespace_only_input_returns_none() {
    let mut parser = Parser::new("   \n\n  ;; just a comment\n   ");
    let result = parser
        .next_request()
        .expect("whitespace parse must succeed");
    assert!(result.is_none(), "whitespace + comments must yield None");
}

#[test]
fn parses_assert_node() {
    let mut parser = Parser::new("(Node [User])");
    let request = parser
        .next_request()
        .expect("Assert parse must succeed")
        .expect("must yield a request");
    assert_eq!(
        request,
        Request::Assert(AssertOperation::Node(Node {
            name: "User".to_string()
        }))
    );
    assert!(parser.next_request().expect("EOF").is_none());
}

#[test]
fn apostrophe_node_names_do_not_require_quote_delimiters() {
    let mut parser = Parser::new("(Node [would replace this node's name])");
    let request = parser
        .next_request()
        .expect("Assert parse must succeed")
        .expect("must yield a request");
    assert_eq!(
        request,
        Request::Assert(AssertOperation::Node(Node {
            name: "would replace this node's name".to_string()
        }))
    );
}

#[test]
fn parses_assert_edge_with_relation_kind() {
    let mut parser = Parser::new("(Edge 100 200 DependsOn)");
    let request = parser
        .next_request()
        .expect("Assert Edge parse must succeed")
        .expect("must yield a request");
    assert_eq!(
        request,
        Request::Assert(AssertOperation::Edge(Edge {
            from: Slot::from(100u64),
            to: Slot::from(200u64),
            kind: RelationKind::DependsOn,
        }))
    );
}

#[test]
fn parses_two_requests_in_sequence() {
    let mut parser = Parser::new("(Node [User]) (Edge 100 200 DependsOn)");

    let first = parser.next_request().expect("first").expect("yields");
    assert!(matches!(first, Request::Assert(AssertOperation::Node(_))));

    let second = parser.next_request().expect("second").expect("yields");
    assert!(matches!(second, Request::Assert(AssertOperation::Edge(_))));

    assert!(parser.next_request().expect("EOF").is_none());
}

#[test]
fn retired_tilde_sigils_are_rejected_by_codec() {
    let mut parser = Parser::new("~(Node [User])");
    let result = parser.next_request();
    assert!(matches!(
        result,
        Err(nexus::Error::Codec(nota_codec::Error::UnexpectedChar {
            character: '~',
            ..
        }))
    ));
}

#[test]
fn retired_bang_sigils_are_rejected_by_codec() {
    let mut parser = Parser::new("!(Node [User])");
    let result = parser.next_request();
    assert!(matches!(
        result,
        Err(nexus::Error::Codec(
            nota_codec::Error::ReservedComparisonToken { token: '!', .. }
        )),
    ));
}

#[test]
fn retired_question_sigils_are_rejected_by_codec() {
    let mut parser = Parser::new("?(Node [User])");
    let result = parser.next_request();
    assert!(matches!(
        result,
        Err(nexus::Error::Codec(nota_codec::Error::UnexpectedChar {
            character: '?',
            ..
        })),
    ));
}

#[test]
fn retired_star_sigils_are_rejected_by_codec() {
    let mut parser = Parser::new("*(Node [User])");
    let result = parser.next_request();
    assert!(matches!(
        result,
        Err(nexus::Error::Codec(nota_codec::Error::UnexpectedChar {
            character: '*',
            ..
        })),
    ));
}

#[test]
fn retired_atomic_delimiters_are_rejected_by_codec() {
    let mut parser = Parser::new("[| (Node [A]) (Node [B]) |]");
    let result = parser.next_request();
    assert!(matches!(
        result,
        Err(nexus::Error::Codec(
            nota_codec::Error::UnexpectedToken { .. }
        )),
    ));
}
