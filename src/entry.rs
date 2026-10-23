//! The standard Nexus entry point.
//!
//! It builds the main, signal, operation, and memory actors once. The only
//! edges are the handles constructed here: signal reaches operation;
//! operation reaches memory. A signal implementation receives neither an
//! admission nor a memory handle.

use std::{
    error::Error,
    future::Future,
    path::{Path, PathBuf},
    sync::Arc,
};

use kameo::actor::{Actor, ActorRef, Spawn};
use kameo::message::{Context, Message};
use tokio::net::UnixStream;
use tokio::{sync::watch, task::JoinHandle};

/// Observable process/task lifetime for a Nexus-owned component.
#[derive(Clone, Debug)]
pub enum LifecycleState {
    Starting,
    Running,
    Failed(LifecycleFailure),
    Stopping,
    Stopped,
}

/// Shared, source-preserving failure reported both in state and by join.
pub type LifecycleFailure = Arc<dyn Error + Send + Sync + 'static>;

/// Cooperative cancellation passed from the generic Nexus lifetime to a typed component task.
#[derive(Clone)]
pub struct Cancellation {
    cancelled: watch::Receiver<bool>,
}

impl Cancellation {
    /// Wait until the owning Nexus begins shutdown.
    pub async fn cancelled(&mut self) {
        loop {
            if *self.cancelled.borrow() {
                return;
            }
            if self.cancelled.changed().await.is_err() {
                return;
            }
        }
    }
}

/// A typed component-owned task run under the Nexus process lifetime.
pub trait RunningTask: Send + 'static {
    fn run(
        self,
        cancellation: Cancellation,
    ) -> impl Future<Output = Result<(), LifecycleFailure>> + Send + 'static;
}

/// The default component task for a Nexus with no background work.
pub struct NoTask;

impl RunningTask for NoTask {
    fn run(
        self,
        cancellation: Cancellation,
    ) -> impl Future<Output = Result<(), LifecycleFailure>> + Send + 'static {
        async move {
            let mut cancellation = cancellation;
            cancellation.cancelled().await;
            Ok(())
        }
    }
}

/// A supervised component task and its cancellation/join boundary.
pub struct SupervisedTask {
    cancel: watch::Sender<bool>,
    state_sender: watch::Sender<LifecycleState>,
    state: watch::Receiver<LifecycleState>,
    task: JoinHandle<Result<(), LifecycleFailure>>,
}

impl SupervisedTask {
    /// Start a typed task and publish its lifecycle transitions.
    pub fn start<T: RunningTask>(task: T) -> Result<Self, tokio::runtime::TryCurrentError> {
        let runtime = tokio::runtime::Handle::try_current()?;
        let (cancel, cancelled) = watch::channel(false);
        let (state_tx, state) = watch::channel(LifecycleState::Starting);
        let state_sender = state_tx.clone();
        let task = runtime.spawn(async move {
            let cancellation = Cancellation { cancelled };
            if *cancellation.cancelled.borrow() {
                return Ok(());
            }
            state_tx.send_replace(LifecycleState::Running);
            match task.run(cancellation).await {
                Ok(()) => Ok(()),
                Err(error) => {
                    let failure: LifecycleFailure = Arc::new(error);
                    Err(failure)
                }
            }
        });
        Ok(Self {
            cancel,
            state_sender,
            state,
            task,
        })
    }

    /// Observe the latest lifecycle state.
    pub fn state(&self) -> LifecycleState {
        self.state.borrow().clone()
    }

    /// Subscribe to the latest lifecycle snapshot and future state changes.
    pub fn subscribe(&self) -> watch::Receiver<LifecycleState> {
        self.state.clone()
    }

    /// Request cooperative stop. The caller must still join to establish completion.
    pub fn cancel(&self) {
        self.cancel.send_replace(true);
        let current = self.state.borrow().clone();
        if matches!(current, LifecycleState::Starting | LifecycleState::Running) {
            self.state_sender.send_replace(LifecycleState::Stopping);
        }
    }

    /// Wait for task completion; task panics remain visible as join errors.
    pub async fn join(self) -> Result<(), LifecycleFailure> {
        let Self {
            state_sender, task, ..
        } = self;
        match task.await {
            Ok(Ok(())) => {
                state_sender.send_replace(LifecycleState::Stopped);
                Ok(())
            }
            Ok(Err(failure)) => {
                state_sender.send_replace(LifecycleState::Failed(failure.clone()));
                Err(failure)
            }
            Err(error) => {
                let failure: LifecycleFailure = Arc::new(error);
                state_sender.send_replace(LifecycleState::Failed(failure.clone()));
                Err(failure)
            }
        }
    }
}

/// The result of changing memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Changed {
    Succeeded,
    Failed,
}

/// Authority to open a Nexus's memory. Only this module can construct it.
pub struct Admission {
    directory: PathBuf,
}

impl Admission {
    /// The directory the entry point admitted to the memory implementation.
    pub fn directory(&self) -> &Path {
        &self.directory
    }
}

/// The signal part translates frames into operations and outcomes into frames.
pub trait Signaling: Send + Sync + 'static {
    type Query: Send + 'static;
    type Response: Send + 'static;
    type Operation: Send + 'static;
    type Outcome: Send + 'static;

    fn decode(&self, frame: &[u8]) -> Option<Self::Query>;
    fn encode(&self, response: Self::Response) -> Vec<u8>;
    fn intend(&self, query: Self::Query) -> Self::Operation;
    fn answer(&self, outcome: Self::Outcome) -> Self::Response;
    fn undecodable(&self) -> Self::Response;
}

/// The memory part opens only with an [`Admission`].
pub trait Remembering: Sized + Send + 'static {
    type Change: Send + 'static;
    type Reading: Send + 'static;
    type Remembered: Send + 'static;

    fn open(admission: Admission) -> Option<Self>;
    fn change(&mut self, change: Self::Change) -> Changed;
    fn read(&self, reading: Self::Reading) -> Self::Remembered;
}

/// The operation part is the only concrete part handed memory.
pub trait Operating<M: Remembering>: Send + 'static {
    type Operation: Send + 'static;
    type Outcome: Send + 'static;

    fn perform(
        &mut self,
        operation: Self::Operation,
        memory: &MemoryHandle<M>,
    ) -> impl Future<Output = Self::Outcome> + Send;
}

/// A whole Nexus supplies its three concrete parts and its local addresses.
pub trait Nexus: Send + 'static {
    type Memory: Remembering;
    type Operation: Operating<Self::Memory>;
    type Signal: Signaling<
        Operation = <Self::Operation as Operating<Self::Memory>>::Operation,
        Outcome = <Self::Operation as Operating<Self::Memory>>::Outcome,
    >;

    fn signal() -> Self::Signal;
    fn operation() -> Self::Operation;
    fn directory() -> PathBuf;
    fn socket_path() -> PathBuf;
}

/// The memory actor holds the concrete memory and no other handle.
pub struct MemoryActor<M: Remembering> {
    memory: M,
}

impl<M: Remembering> Actor for MemoryActor<M> {
    type Args = M;
    type Error = Infallible;

    async fn on_start(memory: M, _: ActorRef<Self>) -> Result<Self, Infallible> {
        Ok(Self { memory })
    }
}

struct Change<C>(C);
struct Reading<R>(R);

impl<M: Remembering> Message<Change<M::Change>> for MemoryActor<M> {
    type Reply = Result<Changed, Infallible>;

    async fn handle(
        &mut self,
        change: Change<M::Change>,
        _: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        Ok(self.memory.change(change.0))
    }
}

impl<M: Remembering> Message<Reading<M::Reading>> for MemoryActor<M> {
    type Reply = Result<M::Remembered, Infallible>;

    async fn handle(
        &mut self,
        reading: Reading<M::Reading>,
        _: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        Ok(self.memory.read(reading.0))
    }
}

/// The only way to memory. Its actor field is private to this module.
pub struct MemoryHandle<M: Remembering> {
    actor: ActorRef<MemoryActor<M>>,
}

impl<M: Remembering> MemoryHandle<M> {
    pub async fn change(&self, change: M::Change) -> Changed {
        self.actor
            .ask(Change(change))
            .await
            .unwrap_or(Changed::Failed)
    }

    pub async fn read(&self, reading: M::Reading) -> Option<M::Remembered> {
        self.actor.ask(Reading(reading)).await.ok()
    }
}

type OperationOf<N> = <<N as Nexus>::Operation as Operating<<N as Nexus>::Memory>>::Operation;
type OutcomeOf<N> = <<N as Nexus>::Operation as Operating<<N as Nexus>::Memory>>::Outcome;

/// The operation actor owns the one memory handle.
pub struct OperationActor<N: Nexus> {
    operation: N::Operation,
    memory: MemoryHandle<N::Memory>,
}

impl<N: Nexus> Actor for OperationActor<N> {
    type Args = (N::Operation, MemoryHandle<N::Memory>);
    type Error = Infallible;

    async fn on_start(
        (operation, memory): Self::Args,
        _: ActorRef<Self>,
    ) -> Result<Self, Infallible> {
        Ok(Self { operation, memory })
    }
}

struct Perform<O>(O);

impl<N: Nexus> Message<Perform<OperationOf<N>>> for OperationActor<N> {
    type Reply = Result<OutcomeOf<N>, Infallible>;

    async fn handle(
        &mut self,
        perform: Perform<OperationOf<N>>,
        _: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        Ok(self.operation.perform(perform.0, &self.memory).await)
    }
}

/// The only way to operation. Main and signal hold it.
pub struct OperationHandle<N: Nexus> {
    actor: ActorRef<OperationActor<N>>,
}

impl<N: Nexus> Clone for OperationHandle<N> {
    fn clone(&self) -> Self {
        Self {
            actor: self.actor.clone(),
        }
    }
}

impl<N: Nexus> OperationHandle<N> {
    pub async fn perform(&self, operation: OperationOf<N>) -> Option<OutcomeOf<N>> {
        self.actor.ask(Perform(operation)).await.ok()
    }
}

/// The signal actor holds operation and never memory.
pub struct SignalActor<N: Nexus> {
    signal: N::Signal,
    operation: OperationHandle<N>,
}

impl<N: Nexus> Actor for SignalActor<N> {
    type Args = (N::Signal, OperationHandle<N>);
    type Error = Infallible;

    async fn on_start(
        (signal, operation): Self::Args,
        _: ActorRef<Self>,
    ) -> Result<Self, Infallible> {
        Ok(Self { signal, operation })
    }
}

struct Frame(Vec<u8>);

impl<N: Nexus> Message<Frame> for SignalActor<N> {
    type Reply = Result<Vec<u8>, Infallible>;

    async fn handle(&mut self, frame: Frame, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let response = match self.signal.decode(&frame.0) {
            Some(query) => match self.operation.perform(self.signal.intend(query)).await {
                Some(outcome) => self.signal.answer(outcome),
                None => self.signal.undecodable(),
            },
            None => self.signal.undecodable(),
        };
        Ok(self.signal.encode(response))
    }
}

/// The socket-facing address of the signal actor.
pub struct Door<N: Nexus> {
    signal: ActorRef<SignalActor<N>>,
}

impl<N: Nexus> Door<N> {
    /// Hand a decoded transport frame to signal.
    pub async fn knock(&self, frame: Vec<u8>) -> Vec<u8> {
        self.signal.ask(Frame(frame)).await.unwrap_or_default()
    }

    /// Serve one length-prefixed request and response over a Unix stream.
    pub async fn serve(&self, mut stream: UnixStream) -> std::io::Result<()> {
        let frame = read_frame(&mut stream).await?;
        let response = self.knock(frame).await;
        write_frame(&mut stream, &response).await
    }
}

/// The main actor keeps operation only.
pub struct MainActor<N: Nexus> {
    operation: OperationHandle<N>,
}

impl<N: Nexus> MainActor<N> {
    pub fn operation(&self) -> &OperationHandle<N> {
        &self.operation
    }
}

/// The one construction and serving path for every Nexus.
pub trait Entering: Nexus + Sized {
    fn open(directory: PathBuf) -> Option<(MainActor<Self>, Door<Self>)> {
        let memory = Self::Memory::open(Admission { directory })?;
        let memory = MemoryHandle {
            actor: MemoryActor::spawn(memory),
        };
        let operation = OperationHandle {
            actor: OperationActor::<Self>::spawn((Self::operation(), memory)),
        };
        let signal = SignalActor::<Self>::spawn((Self::signal(), operation.clone()));
        Some((MainActor { operation }, Door { signal }))
    }

    /// Run the Unix listener until Ctrl-C.
    fn enter() -> std::process::ExitCode {
        Self::enter_with(NoTask)
    }

    /// Run the listener and a typed component task under one process lifetime.
    fn enter_with<T: RunningTask>(task: T) -> std::process::ExitCode {
        let Ok(runtime) = tokio::runtime::Runtime::new() else {
            return std::process::ExitCode::FAILURE;
        };
        runtime.block_on(async {
            let Some((_main, door)) = Self::open(Self::directory()) else {
                return std::process::ExitCode::FAILURE;
            };
            let path = Self::socket_path();
            if path.exists() && std::fs::remove_file(&path).is_err() {
                return std::process::ExitCode::FAILURE;
            }
            let Ok(listener) = tokio::net::UnixListener::bind(&path) else {
                return std::process::ExitCode::FAILURE;
            };
            let Ok(mut terminate) =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            else {
                let _ = std::fs::remove_file(&path);
                return std::process::ExitCode::FAILURE;
            };
            let Ok(mut supervised) = SupervisedTask::start(task) else {
                let _ = std::fs::remove_file(&path);
                return std::process::ExitCode::FAILURE;
            };
            enum ExitCause {
                Shutdown(std::io::Result<()>),
                Component(Result<Result<(), LifecycleFailure>, tokio::task::JoinError>),
                Listener(std::io::Error),
            }
            let cause = loop {
                tokio::select! {
                    signal = async {
                        tokio::select! {
                            ctrl_c = tokio::signal::ctrl_c() => ctrl_c,
                            terminated = terminate.recv() => match terminated {
                                Some(()) => Ok(()),
                                None => Err(std::io::Error::other("SIGTERM listener closed")),
                            },
                        }
                    } => break ExitCause::Shutdown(signal),
                    joined = &mut supervised.task => break ExitCause::Component(joined),
                    accepted = listener.accept() => match accepted {
                        Ok((stream, _)) => { let _ = door.serve(stream).await; }
                        Err(error) => break ExitCause::Listener(error),
                    },
                }
            };
            drop(listener);
            match cause {
                ExitCause::Shutdown(signal) => {
                    let signal_ok = signal.is_ok();
                    let state_sender = supervised.state_sender.clone();
                    supervised.cancel();
                    let joined = supervised.join().await;
                    let _ = std::fs::remove_file(&path);
                    if let Err(error) = signal {
                        state_sender.send_replace(LifecycleState::Failed(Arc::new(error)));
                    }
                    if signal_ok && joined.is_ok() {
                        std::process::ExitCode::SUCCESS
                    } else {
                        std::process::ExitCode::FAILURE
                    }
                }
                ExitCause::Component(joined) => {
                    match joined {
                        Ok(Ok(())) => {
                            supervised
                                .state_sender
                                .send_replace(LifecycleState::Stopped);
                        }
                        Ok(Err(failure)) => {
                            supervised
                                .state_sender
                                .send_replace(LifecycleState::Failed(failure));
                        }
                        Err(error) => {
                            supervised
                                .state_sender
                                .send_replace(LifecycleState::Failed(Arc::new(error)));
                        }
                    }
                    let failed = matches!(supervised.state(), LifecycleState::Failed(_));
                    let _ = std::fs::remove_file(&path);
                    if failed {
                        std::process::ExitCode::FAILURE
                    } else {
                        std::process::ExitCode::SUCCESS
                    }
                }
                ExitCause::Listener(error) => {
                    let state_sender = supervised.state_sender.clone();
                    supervised.cancel();
                    let _ = supervised.join().await;
                    state_sender.send_replace(LifecycleState::Failed(Arc::new(error)));
                    let _ = std::fs::remove_file(&path);
                    std::process::ExitCode::FAILURE
                }
            }
        })
    }
}

impl<N: Nexus> Entering for N {}

async fn read_frame(stream: &mut UnixStream) -> std::io::Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    let mut length = [0; 4];
    stream.read_exact(&mut length).await?;
    let mut frame = vec![0; u32::from_be_bytes(length) as usize];
    stream.read_exact(&mut frame).await?;
    Ok(frame)
}

async fn write_frame(stream: &mut UnixStream, frame: &[u8]) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    let length =
        u32::try_from(frame.len()).map_err(|_| std::io::Error::other("frame too large"))?;
    stream.write_all(&length.to_be_bytes()).await?;
    stream.write_all(frame).await?;
    stream.flush().await
}

/// Make a Nexus's daemon binary its standard entry point.
#[macro_export]
macro_rules! main {
    ($nexus:ty) => {
        fn main() -> std::process::ExitCode {
            <$nexus as $crate::Entering>::enter()
        }
    };
}
