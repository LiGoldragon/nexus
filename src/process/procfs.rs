use crate::{Ancestry, Process, Unidentified};
use std::collections::BTreeSet;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Observation {
    pub process: Process,
    pub parent: i64,
}

pub(super) trait Parsing: Sized {
    fn parse(pid: i64, bytes: &[u8]) -> Result<Self, Unidentified>;
}

impl Parsing for Observation {
    fn parse(pid: i64, bytes: &[u8]) -> Result<Self, Unidentified> {
        if pid <= 0 || pid > i64::from(i32::MAX) {
            return Err(Unidentified::Malformed);
        }
        // comm is arbitrary bytes and can contain spaces, newlines and ')'.
        // Locate its final delimiter before interpreting numeric fields.
        let open = bytes.iter().position(|b| *b == b'(')
            .ok_or(Unidentified::Malformed)?;
        let close = bytes.windows(2).rposition(|pair| pair == b") ")
            .ok_or(Unidentified::Malformed)?;
        if open == 0 || close < open || bytes[open - 1] != b' ' {
            return Err(Unidentified::Malformed);
        }
        let declared = std::str::from_utf8(&bytes[..open - 1])
            .ok().and_then(|value| value.parse::<i64>().ok())
            .ok_or(Unidentified::Malformed)?;
        if declared != pid {
            return Err(Unidentified::Malformed);
        }
        let tail = std::str::from_utf8(&bytes[close + 2..])
            .map_err(|_| Unidentified::Malformed)?;
        let fields: Vec<_> = tail.split_ascii_whitespace().collect();
        let state = *fields.first().ok_or(Unidentified::Malformed)?;
        if matches!(state, "Z" | "X" | "x") {
            return Err(Unidentified::Vanished);
        }
        if !matches!(state, "R" | "S" | "D" | "T" | "t" | "W" | "K" | "P" | "I") {
            return Err(Unidentified::Malformed);
        }
        let parent = fields.get(1).and_then(|value| value.parse::<i64>().ok())
            .ok_or(Unidentified::Malformed)?;
        let started = fields.get(19).and_then(|value| value.parse::<i64>().ok())
            .ok_or(Unidentified::Malformed)?;
        if parent < 0 || parent > i64::from(i32::MAX) || started < 0 {
            return Err(Unidentified::Malformed);
        }
        Ok(Self { process: Process { pid, started }, parent })
    }
}

pub(super) struct Procfs;

pub(super) trait ReadingProcesses {
    fn read(&self, pid: i64) -> Result<Observation, Unidentified>;
}

impl ReadingProcesses for Procfs {
    fn read(&self, pid: i64) -> Result<Observation, Unidentified> {
        if pid <= 0 || pid > i64::from(i32::MAX) {
            return Err(Unidentified::Malformed);
        }
        let bytes = std::fs::read(format!("/proc/{pid}/stat")).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                Unidentified::Vanished
            } else {
                Unidentified::Malformed
            }
        })?;
        Observation::parse(pid, &bytes)
    }
}

pub(super) trait Collecting: ReadingProcesses {
    fn sweep(&self, expected: &Process) -> Result<Vec<Observation>, Unidentified> {
        let mut pid = expected.pid;
        let mut seen = BTreeSet::new();
        let mut chain = Vec::new();
        // Refuse cycles and bound work on a malformed or hostile claim.
        for _ in 0..4096 {
            if !seen.insert(pid) {
                return Err(Unidentified::Malformed);
            }
            let observation = self.read(pid)?;
            if chain.is_empty() && observation.process != *expected {
                return Err(Unidentified::Reused);
            }
            let parent = observation.parent;
            chain.push(observation);
            if pid == 1 {
                return if parent == 0 { Ok(chain) } else { Err(Unidentified::Malformed) };
            }
            if parent == 0 {
                // A procfs view not rooted in our namespace cannot prove init.
                return Err(Unidentified::Malformed);
            }
            pid = parent;
        }
        Err(Unidentified::Malformed)
    }

    fn collect(&self, expected: &Process) -> Result<Ancestry, Unidentified> {
        let first = self.sweep(expected)?;
        let second = self.sweep(expected)?;
        if first != second {
            let reused = first.iter().any(|before| {
                second.iter().any(|after| before.process.pid == after.process.pid
                    && before.process.started != after.process.started)
            });
            return Err(if reused { Unidentified::Reused } else { Unidentified::Malformed });
        }
        Ok(second.into_iter().map(|entry| entry.process).collect())
    }

    fn witness(&self, peer_pid: i64, claimed: &Ancestry) -> Result<Ancestry, Unidentified> {
        let first = claimed.first().ok_or(Unidentified::Malformed)?;
        if first.pid != peer_pid {
            return Err(Unidentified::PeerMismatch);
        }
        if claimed.len() > 4096 {
            return Err(Unidentified::Malformed);
        }
        let observed = self.collect(first)?;
        if observed != *claimed {
            let reused = observed.iter().zip(claimed).any(|(actual, claim)| {
                actual.pid == claim.pid && actual.started != claim.started
            });
            return Err(if reused { Unidentified::Reused } else { Unidentified::Malformed });
        }
        Ok(observed)
    }
}

impl<T: ReadingProcesses> Collecting for T {}
