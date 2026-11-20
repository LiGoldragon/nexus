//! The typed emission sender and async adapters for ordinary Signal frames.
//! No text or Datom dependency is needed by either end of this transport.
use crate::Unobserved;
use signal::{
    FRAME_PREFIX_BYTES, Framable, FrameBody, FrameCapacity, FrameError, FramePrefix,
    LengthDeclaring, Restorable, Signal, Signalizable,
};
use std::{future::Future, path::PathBuf};
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
impl<W: AsyncWrite + Unpin + Send> SignalSubmitting for W {
    async fn submit<T: WireValue>(
        &mut self,
        value: T,
        capacity: FrameCapacity,
    ) -> Result<(), TransmissionError> {
        let bytes = value.framed_value(capacity)?;
        self.write_all(&bytes)
            .await
            .map_err(|error| TransmissionError::Frame(FrameError::Io(error)))
    }
}

/// OS routing and frame admission for a typed emission call.
pub struct EmissionSender {
    pub socket_path: PathBuf,
    pub frame_capacity: FrameCapacity,
}

/// One typed emission, one connection, one typed terminal outcome.
pub trait Sending {
    fn send<Emission: WireValue, Outcome: WireValue>(
        &self,
        emission: Emission,
    ) -> impl Future<Output = Result<Outcome, Unobserved>> + Send;
}
impl Sending for EmissionSender {
    async fn send<Emission: WireValue, Outcome: WireValue>(
        &self,
        emission: Emission,
    ) -> Result<Outcome, Unobserved> {
        let mut connection = UnixStream::connect(&self.socket_path)
            .await
            .map_err(|_| Unobserved::Unsubmitted)?;
        connection
            .submit(emission, self.frame_capacity)
            .await
            .map_err(|_| Unobserved::Unsubmitted)?;
        connection
            .receive(self.frame_capacity)
            .await
            .map_err(|_| Unobserved::Unanswered)
    }
}
