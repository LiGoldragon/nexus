# nexus

Nexus is a typed semantic vocabulary for Sema requests and replies, written in
ordinary NOTA syntax. It mirrors rkyv in text: records and sequences carry the
structure; Rust schema types carry the meaning.

The core rule:

> Delimiters define structure. Head identifiers define meaning. The receiving
> type decides how a field decodes.

Nexus is not an expression language. It has no operators, no function calls,
and no special delimiter for query forms. A top-level request is always one of
the six closed Signal/Nexus root verb records, or a sequence of such records
forming an atomic multi-operation request. Payload records such as
`NodeQuery`, `Project`, or `Constrain` may appear inside those verbs, but they
are not standalone messages.

---

## 1 · Grammar Surface

The Tier 0 grammar is ordinary Nota syntax. Pattern intent is represented by
typed records in typed positions, not by a separate Nexus dialect.

| Construct | Form | Role |
|---|---|---|
| Record | `(Kind field0 field1 ...)` | Named positional composite |
| Sequence | `[elem0 elem1 ...]` | Ordered collection |
| Bind | `(Bind)` | Pattern bind in a `PatternField<T>` position |
| Wildcard | `(Wildcard)` | Pattern wildcard in a `PatternField<T>` position |
| Path | `Name:Nested` | Nested name separator |
| Comment | `;; ...` | Line comment discarded by the parser |
| Byte literal | `#a1b2c3` | Lowercase even-length hex bytes |
| String | `[text]` | Inline bracket string |
| Multiline string | `[|...|]` | Block bracket string |
| Bare string | `bare-ident` | String when the schema expects `String` |

The lexer token vocabulary is locked at the structural minimum:

```rust
pub enum Token {
    LParen,
    RParen,
    LBracket,
    RBracket,
    Colon,
    Ident(String),
    Bool(bool),
    Int(i128),
    UInt(u128),
    Float(f64),
    Str(String),
    Bytes(Vec<u8>),
}
```

`@`, `_` as wildcard, and piped delimiters are retired syntax. A pure Nota
lexer rejects `@`; `_` is just an identifier and has no wildcard privilege.

---

## 2 · Records

A record opens with `(`, followed by a PascalCase head identifier and positional
fields.

```nexus
(Point 3.0 4.0)
(Node User)
(Node [nexus daemon])
(Edge 100 101 Flow)
(Line (Point 0.0 0.0) (Point 10.0 10.0))
```

Field names do not appear in text. They live in the Rust schema. The receiving
type knows how many fields to read, which types they have, and what each
position means.

Record heads also encode closed enum variants:

```nexus
Flow
(Limit 10)
(Range 0 100)
```

Unit variants may render as a bare PascalCase identifier when the receiving type
expects that enum.

---

## 3 · Sequences

A sequence opens with `[` and closes with `]`.

```nexus
[(Node alice) (Node bob) (Node carol)]
[100 101 102]
[([name] 1) ([age] 2)]
[]
```

The receiving type decides whether the sequence is a `Vec<T>`, `BTreeSet<T>`,
or a map encoded as a sequence of pair records. The wire form does not carry a
separate set or map delimiter.

---

## 4 · Patterns

Patterns are schema-driven. There is no pattern delimiter and no pattern
lexer mode.

The same text delimiter `()` is used for data records and query records. The
receiving type decides whether `(Bind)` and `(Wildcard)` are allowed.

```rust
struct Node {
    name: String,
}

struct NodeQuery {
    name: PatternField<String>,
}
```

| Text | Receiving type | Meaning |
|---|---|---|
| `(Node User)` | `Node` | concrete data record |
| `(NodeQuery (Bind))` | `NodeQuery` | bind the `name` field |
| `(NodeQuery (Wildcard))` | `NodeQuery` | wildcard match |
| `(NodeQuery User)` | `NodeQuery` | concrete field match |
| `(Node (Bind))` | `Node` | parse error |

`(Bind)` means "bind this typed field". The position already carries field
identity, so the text does not repeat a field name. Bind names used by
constraint records, projections, and replies are owned by those surrounding
typed records.

`(Bind)` and `(Wildcard)` are valid only when the receiving type is
`PatternField<T>`. Outside a pattern position they are ordinary records and
must fail if the destination type expects a string, slot, enum, or other
non-pattern value.

---

## 5 · Requests

Every top-level request is a root verb record. Tier 0 uses fully explicit
request heads; a bare top-level domain record is not an implicit assert.

The six root verb heads are:

```text
Assert Mutate Retract Match Subscribe Validate
```

`Atomic` is not a verb. Multi-operation atomic requests are written as a
top-level sequence of root-verb records — the sequence itself is the atomic
unit, mapping directly to `signal-core::Request<Payload>` carrying
`NonEmpty<Operation<Payload>>`.

`Query` is not a root verb. Read-algebra names such as `Constrain`, `Project`,
`Aggregate`, `Infer`, and `Recurse` are also not root verbs. They are typed
payload records inside the root that owns the behavior, usually `Match`,
`Subscribe`, or `Validate`.

```rust
pub enum Operation {
    Assert(AssertOperation),
    Mutate(MutateOperation),
    Retract(RetractOperation),
    Match(ReadPlan, Cardinality),
    Subscribe(ReadPlan, SubscriptionMode, Backpressure),
    Validate(ValidateRequest),
}

// A top-level Nexus request is a NonEmpty sequence of Operations.
// Single-op requests render as one verb record; multi-op requests
// render as a top-level sequence of verb records.
pub struct Request {
    pub operations: NonEmpty<Operation>,
}

pub enum ReadPlan {
    Pattern(PatternQuery),
    Constrain(ConstrainPlan),
    Project(ProjectPlan),
    Aggregate(AggregatePlan),
    Infer(InferPlan),
    Recurse(RecursePlan),
}
```

Examples:

```nexus
(Assert (Node User))
(Assert (Edge 100 101 Flow))

(Mutate 100 (Node [renamed]))
(Retract Node 100)

;; Multi-operation atomic request — top-level sequence of verb records.
[(Assert (Node A)) (Assert (Node B))]
[(Retract (RoleClaim Designer)) (Assert (RoleClaim Poet))]

(Match (NodeQuery (Bind)) Any)
(Match (EdgeQuery 100 (Bind) Flow) (Limit 10))
(Subscribe (NodeQuery (Bind)) ImmediateExtension Block)
(Validate (Assert (Node [dry run])))

(Match (Aggregate (NodeQuery (Bind)) Count) Any)
(Match (Project (NodeQuery (Bind)) (Fields [name])) Any)
(Match
  (Constrain [(EdgeQuery 100 (Bind) Flow) (NodeQuery (Bind))] (Unify [to]))
  Any)
(Match (Infer (NodeQuery (Bind)) StandardOntology) Any)
(Match
  (Recurse (NodeQuery (Bind)) (EdgeQuery (Bind) (Bind) DependsOn) Fixpoint)
  Any)
```

Slot references are bare integers in slot-typed positions. The schema tells the
decoder that `100` is a `Slot<Node>`, not an ordinary `u64`.

---

## 6 · Replies

Replies are typed records or sequences of typed records.

```nexus
(Ok)
(Diagnostic Error E0042 [no binding for unknown-target])
[(Node User) (Node [nexus daemon])]
[(Ok) (Diagnostic Error E0042 [conflict on slot 100]) (Ok)]
```

When a reply needs to carry the store slot beside a returned record, use a typed
pair record rather than an anonymous tuple:

```nexus
[(SlotBinding 1024 (Node User))
 (SlotBinding 1025 (Node [nexus daemon]))]
```

`SlotBinding<T>` is the textual shape for `slot + value` reply data. It is a
named type because anonymous tuples are not used at typed boundaries.

---

## 7 · Dropped Forms

These forms are not reserved. They are outside Tier 0 and should not appear in
new examples or new parser work.

| Dropped form | Replacement |
|---|---|
| `(| Node @name |)` | `(Match (NodeQuery (Bind)) Any)` |
| `[| op1 op2 |]` | `[op1 op2]` — a top-level NonEmpty sequence of verb records is the atomic unit |
| `{ name }` | `(Match (Project pattern (Fields [name])) cardinality)` |
| `{| pat1 pat2 |}` | `(Match (Constrain [pat1 pat2] (Unify [name])) cardinality)` |
| `~record` | `(Mutate slot record)` |
| `!record` | `(Retract Kind slot)` |
| `?record` | `(Validate request)` |
| `*pattern` | `(Subscribe pattern mode backpressure)` |
| `@name`, `_` | `(Bind)`, `(Wildcard)` in a `PatternField<T>` position |
| `@a=@b` | Deferred; use `(Unify [a b])` where a typed record owns the behavior |
| `< > <= >= !=` | Predicates are typed records |

---

## 8 · Current Daemon Status

The existing `nexus-daemon` implementation is still Criome-specific. The Tier 0
rewrite universalizes the spec first. The daemon becomes domain-parameterized
only after another concrete domain translator needs it.
