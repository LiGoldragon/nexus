//! Renderer tests — `signal::Reply` → text.
//!
//! Asserts the canonical text shape per
//! [nexus/spec/grammar.md §"Reply semantics"](../spec/grammar.md).

use nexus::Renderer;
use signal::{
    Diagnostic, DiagnosticLevel, Edge, Node, Ok, OutcomeMessage, Records, RelationKind, Reply, Slot,
};

#[test]
fn ok_outcome_renders_as_paren_ok_paren() {
    let mut renderer = Renderer::new();
    renderer
        .render_reply(&Reply::Outcome(OutcomeMessage::Ok(Ok::default())))
        .expect("render");
    assert_eq!(renderer.into_text(), "(Ok)");
}

#[test]
fn diagnostic_outcome_renders_with_level_code_message() {
    let mut renderer = Renderer::new();
    renderer
        .render_reply(&Reply::Outcome(OutcomeMessage::Diagnostic(Diagnostic {
            level: DiagnosticLevel::Error,
            code: "E0042".to_string(),
            message: "thing went wrong".to_string(),
            primary_site: None,
            context: vec![],
            suggestions: vec![],
            durable_record: None,
        })))
        .expect("render");
    assert_eq!(
        renderer.into_text(),
        "(Diagnostic Error E0042 [thing went wrong])",
    );
}

#[test]
fn diagnostic_messages_with_apostrophes_render_as_bracket_strings() {
    let mut renderer = Renderer::new();
    renderer
        .render_reply(&Reply::Outcome(OutcomeMessage::Diagnostic(Diagnostic {
            level: DiagnosticLevel::Error,
            code: "E0043".to_string(),
            message: "thing's wrong".to_string(),
            primary_site: None,
            context: vec![],
            suggestions: vec![],
            durable_record: None,
        })))
        .expect("render");
    assert_eq!(
        renderer.into_text(),
        "(Diagnostic Error E0043 [thing's wrong])",
    );
}

#[test]
fn outcomes_sequence_renders_as_bracketed_seq() {
    let mut renderer = Renderer::new();
    renderer
        .render_reply(&Reply::Outcomes(vec![
            OutcomeMessage::Ok(Ok::default()),
            OutcomeMessage::Ok(Ok::default()),
        ]))
        .expect("render");
    assert_eq!(renderer.into_text(), "[(Ok) (Ok)]");
}

#[test]
fn empty_records_node_renders_as_empty_seq() {
    let mut renderer = Renderer::new();
    renderer
        .render_reply(&Reply::Records(Records::Node(vec![])))
        .expect("render");
    assert_eq!(renderer.into_text(), "[]");
}

#[test]
fn populated_records_node_renders_each_node_as_record() {
    let mut renderer = Renderer::new();
    renderer
        .render_reply(&Reply::Records(Records::Node(vec![
            (
                Slot::from(1024u64),
                Node {
                    name: "User".to_string(),
                },
            ),
            (
                Slot::from(1025u64),
                Node {
                    name: "Group".to_string(),
                },
            ),
        ])))
        .expect("render");
    assert_eq!(
        renderer.into_text(),
        "[(SlotBinding 1024 (Node User)) (SlotBinding 1025 (Node Group))]"
    );
}

#[test]
fn populated_records_edge_renders_each_with_relation_kind() {
    let mut renderer = Renderer::new();
    renderer
        .render_reply(&Reply::Records(Records::Edge(vec![(
            Slot::from(1029u64),
            Edge {
                from: Slot::from(100u64),
                to: Slot::from(200u64),
                kind: RelationKind::Calls,
            },
        )])))
        .expect("render");
    assert_eq!(
        renderer.into_text(),
        "[(SlotBinding 1029 (Edge 100 200 Calls))]"
    );
}

#[test]
fn handshake_accepted_in_user_reply_stream_is_a_protocol_error() {
    let mut renderer = Renderer::new();
    let result = renderer.render_reply(&Reply::HandshakeAccepted(signal::HandshakeReply {
        server_version: signal::SIGNAL_PROTOCOL_VERSION,
        server_id: Slot::from(0u64),
    }));
    assert!(matches!(
        result,
        Err(nexus::Error::HandshakePostReplyShape {
            got: "HandshakeAccepted"
        }),
    ));
}

#[test]
fn two_replies_separate_with_newline() {
    let mut renderer = Renderer::new();
    renderer
        .render_reply(&Reply::Outcome(OutcomeMessage::Ok(Ok::default())))
        .expect("first");
    renderer
        .render_reply(&Reply::Records(Records::Node(vec![(
            Slot::from(1024u64),
            Node {
                name: "User".to_string(),
            },
        )])))
        .expect("second");
    assert_eq!(
        renderer.into_text(),
        "(Ok)\n[(SlotBinding 1024 (Node User))]"
    );
}

#[test]
fn local_error_renders_as_diagnostic_with_code() {
    let mut renderer = Renderer::new();
    renderer
        .render_local_error(&nexus::Error::VerbNotInM0Scope { verb: "Mutate" })
        .expect("render");
    let text = renderer.into_text();
    assert!(text.starts_with("(Diagnostic Error E0099"));
    assert!(text.contains("Mutate"));
}
