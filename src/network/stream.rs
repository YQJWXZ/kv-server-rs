use std::{
    marker::PhantomData,
    pin::Pin,
    task::{ready, Context, Poll},
};

use bytes::{Buf, BytesMut};
use futures::{FutureExt, Sink, Stream};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{read_frame, KvError};

use super::FrameCoder;

/// handle stream of KV server prost frame
pub struct ProstStream<S, In, Out> {
    stream: S,
    // write buffer
    wbuf: BytesMut,
    // read buffer
    rbuf: BytesMut,

    _in: PhantomData<In>,
    _out: PhantomData<Out>,
}

impl<S, In, Out> Stream for ProstStream<S, In, Out>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    In: Unpin + Send + FrameCoder,
    Out: Unpin + Send,
{
    type Item = Result<In, KvError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        assert!(self.rbuf.is_empty());

        let mut rest = self.rbuf.split_off(0);
        let fut = read_frame(&mut self.stream, &mut rest);

        // Because the poll_xxx() method is already an underlying API implementation of async/await,
        // it cannot directly use asynchronous functions in the poll_xxx() method,
        // we need to regard it as a future and then call the poll function of future.
        // Because future is a trait, the Box needs to process it into a trait object on the heap,
        // so that the poll_unpin() method of FutureExt can be called.
        // Box:pin() generates a Pin.
        ready!(Box::pin(fut).poll_unpin(cx))?;

        self.rbuf.unsplit(rest);

        Poll::Ready(Some(In::decode_frame(&mut self.rbuf)))
    }
}

impl<S, In, Out> Sink<&Out> for ProstStream<S, In, Out>
where
    S: AsyncRead + AsyncWrite + Unpin,
    In: Unpin + Send,
    Out: Unpin + Send + FrameCoder,
{
    type Error = KvError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if !self.as_mut().get_mut().wbuf.is_empty() {
            self.as_mut().poll_flush(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn start_send(self: Pin<&mut Self>, item: &Out) -> Result<(), Self::Error> {
        let this = self.get_mut();
        item.encode_frame(&mut this.wbuf)?;
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        while !this.wbuf.is_empty() {
            let n = ready!(Pin::new(&mut this.stream).poll_write(cx, &this.wbuf))?;
            if n == 0 {
                return Poll::Ready(Err(KvError::Internal("unexpected EOF".into())));
            }
            this.wbuf.advance(n);
        }

        this.wbuf.clear();

        ready!(Pin::new(&mut this.stream).poll_flush(cx))?;
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().poll_flush(cx))?;
        let stream = &mut self.get_mut().stream;
        ready!(Pin::new(stream).poll_shutdown(cx))?;
        Poll::Ready(Ok(()))
    }
}

impl<S, In, Out> ProstStream<S, In, Out>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            wbuf: BytesMut::new(),
            rbuf: BytesMut::new(),
            _in: PhantomData,
            _out: PhantomData,
        }
    }
}

impl<S, Req, Res> Unpin for ProstStream<S, Req, Res> where S: Unpin {}
