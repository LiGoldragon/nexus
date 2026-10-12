use super::procfs::{Collecting, Observation, Parsing, ReadingProcesses};
use crate::{Process, Unidentified};
use std::{cell::RefCell, collections::BTreeMap};

struct Fixture {
    records: BTreeMap<i64, Observation>,
    replacement: Option<(usize, Observation)>,
    reads: RefCell<usize>,
}

trait FixtureFactory: Sized {
    fn wrapper() -> Self;
    fn ancestry(&self) -> Vec<Process>;
}

impl FixtureFactory for Fixture {
    fn wrapper() -> Self {
        Self {
            records: [
                (
                    90,
                    Observation {
                        process: Process {
                            pid: 90,
                            started: 900,
                        },
                        parent: 80,
                    },
                ),
                (
                    80,
                    Observation {
                        process: Process {
                            pid: 80,
                            started: 800,
                        },
                        parent: 70,
                    },
                ),
                (
                    70,
                    Observation {
                        process: Process {
                            pid: 70,
                            started: 700,
                        },
                        parent: 1,
                    },
                ),
                (
                    1,
                    Observation {
                        process: Process { pid: 1, started: 1 },
                        parent: 0,
                    },
                ),
            ]
            .into(),
            replacement: None,
            reads: RefCell::new(0),
        }
    }

    fn ancestry(&self) -> Vec<Process> {
        [90, 80, 70, 1]
            .iter()
            .map(|pid| self.records[pid].process.clone())
            .collect()
    }
}

impl ReadingProcesses for Fixture {
    fn read(&self, pid: i64) -> Result<Observation, Unidentified> {
        let mut reads = self.reads.borrow_mut();
        *reads += 1;
        if let Some((after, replacement)) = &self.replacement {
            if *reads > *after && replacement.process.pid == pid {
                return Ok(replacement.clone());
            }
        }
        self.records
            .get(&pid)
            .cloned()
            .ok_or(Unidentified::Vanished)
    }
}

#[test]
fn wrapper_edges_are_preserved_and_no_ancestor_is_substituted_for_peer() {
    let fixture = Fixture::wrapper();
    let ancestry = fixture.ancestry();
    assert_eq!(fixture.witness(90, &ancestry), Ok(ancestry.clone()));
    assert_eq!(
        fixture.witness(90, &ancestry[1..].to_vec()),
        Err(Unidentified::PeerMismatch)
    );
}

#[test]
fn unrelated_live_process_is_not_a_caller_or_a_claimed_parent() {
    let mut fixture = Fixture::wrapper();
    let unrelated = Process {
        pid: 40,
        started: 400,
    };
    fixture.records.insert(
        40,
        Observation {
            process: unrelated.clone(),
            parent: 1,
        },
    );
    assert_eq!(
        fixture.witness(90, &vec![unrelated.clone()]),
        Err(Unidentified::PeerMismatch)
    );
    let mut claim = fixture.ancestry();
    claim[1] = unrelated;
    assert_eq!(fixture.witness(90, &claim), Err(Unidentified::Malformed));
}

#[test]
fn reused_peer_or_ancestor_is_refused() {
    let fixture = Fixture::wrapper();
    let mut claim = fixture.ancestry();
    claim[0].started -= 1;
    assert_eq!(fixture.witness(90, &claim), Err(Unidentified::Reused));
    claim = fixture.ancestry();
    claim[1].started -= 1;
    assert_eq!(fixture.witness(90, &claim), Err(Unidentified::Reused));
}

#[test]
fn vanished_process_is_not_skipped() {
    let mut fixture = Fixture::wrapper();
    let claim = fixture.ancestry();
    fixture.records.remove(&80);
    assert_eq!(fixture.witness(90, &claim), Err(Unidentified::Vanished));
}

#[test]
fn reparent_during_observation_refuses_the_chain() {
    let mut fixture = Fixture::wrapper();
    let mut changed = fixture.records[&80].clone();
    changed.parent = 1;
    fixture.replacement = Some((4, changed));
    assert_eq!(
        fixture.witness(90, &fixture.ancestry()),
        Err(Unidentified::Malformed)
    );
}

#[test]
fn ancestor_reuse_during_observation_refuses_the_chain() {
    let mut fixture = Fixture::wrapper();
    let mut changed = fixture.records[&80].clone();
    changed.process.started += 1;
    fixture.replacement = Some((4, changed));
    assert_eq!(
        fixture.witness(90, &fixture.ancestry()),
        Err(Unidentified::Reused)
    );
}

#[test]
fn missing_init_cycles_and_empty_claims_are_refused() {
    let mut fixture = Fixture::wrapper();
    assert_eq!(fixture.witness(90, &vec![]), Err(Unidentified::Malformed));
    let claim = fixture.ancestry();
    fixture.records.get_mut(&70).unwrap().parent = 0;
    assert_eq!(fixture.witness(90, &claim), Err(Unidentified::Malformed));
    fixture.records.get_mut(&70).unwrap().parent = 90;
    assert_eq!(fixture.witness(90, &claim), Err(Unidentified::Malformed));
}

#[test]
fn stat_parser_handles_spaces_parentheses_newlines_and_non_utf8_comm() {
    // Fields 3 (state), 4 (ppid), then fields 5..21, then 22 (starttime).
    let mut bytes = b"90 (name ) with\n( bytes\xff) S 80".to_vec();
    for _ in 5..22 {
        bytes.extend_from_slice(b" 0");
    }
    bytes.extend_from_slice(b" 900 0\n");
    assert_eq!(
        Observation::parse(90, &bytes),
        Ok(Observation {
            process: Process {
                pid: 90,
                started: 900
            },
            parent: 80,
        })
    );
    assert_eq!(Observation::parse(91, &bytes), Err(Unidentified::Malformed));
}

#[test]
fn malformed_or_dead_stat_never_becomes_a_process() {
    for bytes in [
        b"90 comm S 80".as_slice(),
        b"90 (comm) ? 80",
        b"90 (comm) S 80",
    ] {
        assert_eq!(Observation::parse(90, bytes), Err(Unidentified::Malformed));
    }
    assert_eq!(
        Observation::parse(90, b"90 (comm) Z 80"),
        Err(Unidentified::Vanished)
    );
    assert_eq!(
        Observation::parse(-1, b"-1 (comm) S 0"),
        Err(Unidentified::Malformed)
    );
}
