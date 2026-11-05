use std::{
    path::PathBuf,
    process::{Child, Command, ExitStatus},
    time::{Duration, Instant},
};

use nexus::{
    Admission, Changed, Entering, FrameLimit, MemoryHandle, Nexus, Operating, Remembering,
    Signaling,
};

const DIRECTORY: &str = "NEXUS_PROCESS_FIXTURE_DIRECTORY";
const SOCKET: &str = "NEXUS_PROCESS_FIXTURE_SOCKET";

struct Memory;

impl Remembering for Memory {
    type Change = ();
    type Reading = ();
    type Remembered = ();

    fn open(_: Admission) -> Option<Self> {
        Some(Self)
    }

    fn change(&mut self, _: Self::Change) -> Changed {
        Changed::Succeeded
    }

    fn read(&self, _: Self::Reading) -> Self::Remembered {}
}

struct Operation;

impl Operating<Memory> for Operation {
    type Operation = ();
    type Outcome = ();

    async fn perform(&mut self, _: Self::Operation, _: &MemoryHandle<Memory>) -> Self::Outcome {}
}

struct Signal;

impl Signaling for Signal {
    type Query = ();
    type Response = ();
    type Operation = ();
    type Outcome = ();

    fn decode(&self, _: &[u8]) -> Option<Self::Query> {
        Some(())
    }

    fn encode(&self, _: Self::Response) -> Vec<u8> {
        Vec::new()
    }

    fn intend(&self, _: Self::Query) -> Self::Operation {}

    fn answer(&self, _: Self::Outcome) -> Self::Response {}

    fn undecodable(&self) -> Self::Response {}
}

struct ProcessNexus;

impl Nexus for ProcessNexus {
    type Memory = Memory;
    type Operation = Operation;
    type Signal = Signal;

    fn signal() -> Self::Signal {
        Signal
    }

    fn operation() -> Self::Operation {
        Operation
    }

    fn directory() -> PathBuf {
        std::env::var_os(DIRECTORY).unwrap().into()
    }

    fn socket_path() -> PathBuf {
        std::env::var_os(SOCKET).unwrap().into()
    }
}

struct OwnedChild(Option<Child>);

impl OwnedChild {
    fn child(&mut self) -> &mut Child {
        self.0.as_mut().unwrap()
    }

    fn completed(&mut self, status: ExitStatus) {
        assert!(status.success(), "fixture exited with {status}");
        self.0 = None;
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn wait_until(deadline: Instant, mut ready: impl FnMut() -> bool) {
    while !ready() {
        assert!(Instant::now() < deadline, "fixture condition timed out");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
#[ignore = "subprocess fixture invoked by the owning test"]
fn nexus_process_fixture() {
    assert_eq!(
        ProcessNexus::enter(FrameLimit::new(1024)),
        std::process::ExitCode::SUCCESS
    );
}

#[test]
fn sigterm_stops_the_owned_process_and_removes_its_socket() {
    use std::os::unix::fs::FileTypeExt;

    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("nexus.sock");
    let child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "nexus_process_fixture",
            "--nocapture",
        ])
        .env(DIRECTORY, directory.path())
        .env(SOCKET, &socket)
        .spawn()
        .unwrap();
    let mut child = OwnedChild(Some(child));

    let deadline = Instant::now() + Duration::from_secs(5);
    wait_until(deadline, || {
        assert!(child.child().try_wait().unwrap().is_none());
        std::fs::symlink_metadata(&socket).is_ok_and(|metadata| metadata.file_type().is_socket())
    });

    let pid = rustix::process::Pid::from_child(child.child());
    rustix::process::kill_process(pid, rustix::process::Signal::TERM).unwrap();
    let mut status = None;
    wait_until(Instant::now() + Duration::from_secs(5), || {
        status = child.child().try_wait().unwrap();
        status.is_some()
    });
    child.completed(status.unwrap());
    assert!(!socket.exists());
}
