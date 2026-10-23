use std::{error::Error, fmt, future::Future, sync::Arc, time::Duration};

use nexus::{Cancellation, LifecycleFailure, LifecycleState, RunningTask, SupervisedTask};

struct WaitForCancellation;

impl RunningTask for WaitForCancellation {
    fn run(
        self,
        mut cancellation: Cancellation,
    ) -> impl Future<Output = Result<(), LifecycleFailure>> + Send + 'static {
        async move {
            cancellation.cancelled().await;
            Ok(())
        }
    }
}

#[derive(Debug)]
struct TestFailure(std::io::Error);

impl fmt::Display for TestFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "projection failed: {}", self.0)
    }
}

impl Error for TestFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}

struct Fail(tokio::sync::oneshot::Sender<()>);

impl RunningTask for Fail {
    fn run(
        self,
        _cancellation: Cancellation,
    ) -> impl Future<Output = Result<(), LifecycleFailure>> + Send + 'static {
        let failure: LifecycleFailure = Arc::new(TestFailure(std::io::Error::other("root cause")));
        let completed = self.0;
        async move {
            let _ = completed.send(());
            Err(failure)
        }
    }
}

#[test]
fn cancellation_is_cooperative_and_joinable() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let task = SupervisedTask::start(WaitForCancellation).unwrap();
        let mut states = task.subscribe();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if matches!(*states.borrow_and_update(), LifecycleState::Running) {
                    break;
                }
                states.changed().await.unwrap();
            }
        })
        .await
        .unwrap();

        task.cancel();
        assert!(matches!(*states.borrow(), LifecycleState::Stopping));
        tokio::time::timeout(Duration::from_secs(1), task.join())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(*states.borrow(), LifecycleState::Stopped));
    });
}

#[test]
fn task_failure_is_reported_with_its_reason() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let (complete, completed) = tokio::sync::oneshot::channel();
        let task = SupervisedTask::start(Fail(complete)).unwrap();
        let mut states = task.subscribe();
        completed.await.unwrap();
        assert!(matches!(*states.borrow(), LifecycleState::Running));
        let joined_failure = task.join().await.unwrap_err();
        let LifecycleState::Failed(state_failure) = states.borrow_and_update().clone() else {
            panic!("expected typed failure state");
        };
        assert!(Arc::ptr_eq(&joined_failure, &state_failure));
        assert_eq!(joined_failure.to_string(), "projection failed: root cause");
        assert_eq!(joined_failure.source().unwrap().to_string(), "root cause");
    });
}
