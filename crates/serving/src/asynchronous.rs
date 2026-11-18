//! The asynchronous half of the exchange layer, on which a Nexus serves its
//! public socket.

use std::future::Future;
use std::time::Duration;

use nexus_exchange::ExchangeError;
use signal::{Capacious, FRAME_PREFIX_BYTES, FrameBody, FrameCapacity, FrameError, Signalizable};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// An asynchronous connection read one whole Signal frame at a time.
pub trait AsyncFrameReading {
    /// Reads one frame as `IdleFrameReading` reads one from a blocking
    /// stream: the wait for its first byte is unbounded and the rest of it
    /// has one absolute progress deadline. `None` is the peer closing between
    /// frames.
    fn read_frame_after_idle_async(
        &mut self,
        progress: Duration,
        capacity: FrameCapacity,
    ) -> impl Future<Output = Result<Option<FrameBody>, FrameError>> + Send;

    /// Reads one frame, the whole of it within `deadline`. `None` is the peer
    /// closing before the frame began.
    fn read_frame_within_async(
        &mut self,
        deadline: Duration,
        capacity: FrameCapacity,
    ) -> impl Future<Output = Result<Option<FrameBody>, FrameError>> + Send;
}

impl<R: AsyncRead + Unpin + Send> AsyncFrameReading for R {
    async fn read_frame_after_idle_async(
        &mut self,
        progress: Duration,
        capacity: FrameCapacity,
    ) -> Result<Option<FrameBody>, FrameError> {
        let mut first = 0_u8;
        if self.read(std::slice::from_mut(&mut first)).await? == 0 {
            return Ok(None);
        }
        tokio::time::timeout(progress, rest_of_frame(first, self, capacity))
            .await
            .map_err(|_| elapsed("frame progress deadline elapsed"))?
            .map(Some)
    }

    async fn read_frame_within_async(
        &mut self,
        deadline: Duration,
        capacity: FrameCapacity,
    ) -> Result<Option<FrameBody>, FrameError> {
        tokio::time::timeout(deadline, async {
            let mut first = 0_u8;
            if self.read(std::slice::from_mut(&mut first)).await? == 0 {
                return Ok(None);
            }
            rest_of_frame(first, self, capacity).await.map(Some)
        })
        .await
        .map_err(|_| elapsed("frame deadline elapsed"))?
    }
}

async fn rest_of_frame<R: AsyncRead + Unpin>(
    first: u8,
    input: &mut R,
    capacity: FrameCapacity,
) -> Result<FrameBody, FrameError> {
    let mut prefix = [0_u8; FRAME_PREFIX_BYTES];
    prefix[0] = first;
    input.read_exact(&mut prefix[1..]).await?;
    let length = capacity.admit(u32::from_be_bytes(prefix) as usize)?;
    let mut body = vec![0_u8; length];
    input.read_exact(&mut body).await?;
    Ok(FrameBody::from(body))
}

/// An asynchronous connection one value of the exchange layer is written to.
pub trait AsyncValueWriting {
    /// Writes the value as one frame, the whole frame within `timeout`.
    fn write_value_async<T: Signalizable>(
        &mut self,
        value: &T,
        timeout: Duration,
        capacity: FrameCapacity,
    ) -> impl Future<Output = Result<(), ExchangeError>> + Send;
}

impl<W: AsyncWrite + Unpin + Send> AsyncValueWriting for W {
    fn write_value_async<T: Signalizable>(
        &mut self,
        value: &T,
        timeout: Duration,
        capacity: FrameCapacity,
    ) -> impl Future<Output = Result<(), ExchangeError>> + Send {
        use signal::Framable;
        let framed = value
            .signalize()
            .map_err(|error| ExchangeError::Archive(format!("{error:?}")))
            .and_then(|signal| signal.framed(capacity).map_err(ExchangeError::from));
        async move {
            let framed = framed?;
            tokio::time::timeout(timeout, async {
                self.write_all(&framed).await?;
                self.flush().await
            })
            .await
            .map_err(|_| elapsed("frame write deadline elapsed"))??;
            Ok(())
        }
    }
}

fn elapsed(what: &'static str) -> FrameError {
    FrameError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, what))
}
