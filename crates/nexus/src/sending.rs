//! The typed emission sender and async adapters for ordinary Signal frames.
//! No text or Datom dependency is needed by either end of this transport.
use crate::Unobserved;
use signal::{
    FRAME_PREFIX_BYTES, Framable, FrameBody, FrameCapacity, FrameError, FramePrefix,
    LengthDeclaring, Restorable, Signal, Signalizable,
};
use std::{future::Future, io, path::PathBuf};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::UnixStream,
};

#[derive(Debug)]
pub enum TransmissionError {
    Frame(FrameError),
    Archive(rkyv::rancor::Error),
}

/// An archived value restored by the shared Signal implementation.
pub trait WireValue: Signalizable + Send + 'static {
    fn restored(body: FrameBody) -> Result<Self, TransmissionError>;
    fn framed_value(&self, capacity: FrameCapacity) -> Result<Vec<u8>, TransmissionError>;
}
impl<T> WireValue for T
where
    T: rkyv::Archive + Signalizable + Send + 'static,
    Signal<T>: Restorable<T>,
{
    fn restored(body: FrameBody) -> Result<Self, TransmissionError> {
        Signal::<T>::from(Vec::from(body))
            .restore()
            .map_err(TransmissionError::Archive)
    }
    fn framed_value(&self, capacity: FrameCapacity) -> Result<Vec<u8>, TransmissionError> {
        self.signalize()
            .map_err(TransmissionError::Archive)?
            .framed(capacity)
            .map_err(TransmissionError::Frame)
    }
}

/// Async I/O over Signal's prefix/body types, not a separate wire codec.
pub trait SignalReceiving {
    fn receive<T: WireValue>(
        &mut self,
        capacity: FrameCapacity,
    ) -> impl Future<Output = Result<T, TransmissionError>> + Send;
}
pub trait SignalSubmitting {
    fn submit<T: WireValue>(
        &mut self,
        value: T,
        capacity: FrameCapacity,
    ) -> impl Future<Output = Result<(), TransmissionError>> + Send;
}
impl<R: AsyncRead + Unpin + Send> SignalReceiving for R {
    async fn receive<T: WireValue>(
        &mut self,
        capacity: FrameCapacity,
    ) -> Result<T, TransmissionError> {
        let mut prefix = [0; FRAME_PREFIX_BYTES];
        self.read_exact(&mut prefix)
            .await
            .map_err(|error| TransmissionError::Frame(FrameError::Io(error)))?;
        let length = FramePrefix::from(prefix)
            .admitted_length(capacity)
            .map_err(TransmissionError::Frame)?;
        let mut body = vec![0; length];
        self.read_exact(&mut body)
            .await
            .map_err(|error| TransmissionError::Frame(FrameError::Io(error)))?;
        T::restored(FrameBody::from(body))
    }
}
/// Writes an already prepared Signal frame without encoding it again.
pub trait FrameSubmitting {
    fn submit_frame(
        &mut self,
        frame: &[u8],
    ) -> impl Future<Output = Result<(), TransmissionError>> + Send;
}
impl<W: AsyncWrite + Unpin + Send> FrameSubmitting for W {
    async fn submit_frame(&mut self, frame: &[u8]) -> Result<(), TransmissionError> {
        self.write_all(frame)
            .await
            .map_err(|error| TransmissionError::Frame(FrameError::Io(error)))
    }
}
impl<W: AsyncWrite + Unpin + Send> SignalSubmitting for W {
    async fn submit<T: WireValue>(
        &mut self,
        value: T,
        capacity: FrameCapacity,
    ) -> Result<(), TransmissionError> {
        let frame = value.framed_value(capacity)?;
        self.submit_frame(&frame).await
    }
}

/// OS routing and frame admission for a typed emission call.
pub struct EmissionSender {
    pub socket_path: PathBuf,
    pub frame_capacity: FrameCapacity,
}

/// The endpoint that establishes an emission connection.
pub trait EmissionConnecting {
    type Connection: AsyncRead + AsyncWrite + Unpin + Send;
    fn connect(&self) -> impl Future<Output = io::Result<Self::Connection>> + Send;
}
impl EmissionConnecting for PathBuf {
    type Connection = UnixStream;
    async fn connect(&self) -> io::Result<Self::Connection> {
        UnixStream::connect(self).await
    }
}

/// One typed emission, one connection, one typed terminal outcome.
pub trait Sending {
    fn send<Emission: WireValue, Outcome: WireValue>(
        &self,
        emission: Emission,
    ) -> impl Future<Output = Result<Outcome, Unobserved>> + Send;
}
/// The same sender over an explicitly supplied connection capability.
pub trait SendingWith {
    fn send_with<Emission: WireValue, Outcome: WireValue, Connector: EmissionConnecting + Sync>(
        &self,
        emission: Emission,
        connector: &Connector,
    ) -> impl Future<Output = Result<Outcome, Unobserved>> + Send;
}
impl SendingWith for EmissionSender {
    async fn send_with<
        Emission: WireValue,
        Outcome: WireValue,
        Connector: EmissionConnecting + Sync,
    >(
        &self,
        emission: Emission,
        connector: &Connector,
    ) -> Result<Outcome, Unobserved> {
        // Preparation touches no receiver. Once connected, every failure is uncertain.
        let frame = emission
            .framed_value(self.frame_capacity)
            .map_err(|_| Unobserved::Unsubmitted)?;
        let mut connection = connector
            .connect()
            .await
            .map_err(|_| Unobserved::Unsubmitted)?;
        connection
            .submit_frame(&frame)
            .await
            .map_err(|_| Unobserved::Unanswered)?;
        connection
            .receive(self.frame_capacity)
            .await
            .map_err(|_| Unobserved::Unanswered)
    }
}
impl Sending for EmissionSender {
    async fn send<Emission: WireValue, Outcome: WireValue>(
        &self,
        emission: Emission,
    ) -> Result<Outcome, Unobserved> {
        self.send_with(emission, &self.socket_path).await
    }
}
