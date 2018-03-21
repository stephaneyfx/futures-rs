use futures_core::{Async, Poll, Stream};
use futures_core::task;
use stream::{Fuse, StreamExt};

/// Adapter to zip two streams using their latest values.
///
/// Return type of the `zip_latest` adapter.
#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub struct ZipLatest<S1: Stream, S2: Stream> {
    stream1: Fuse<S1>,
    stream2: Fuse<S2>,
    queued1: Option<S1::Item>,
    queued1_fresh: bool,
    queued2: Option<S2::Item>,
    queued2_fresh: bool,
}

pub fn new<S1, S2>(stream1: S1, stream2: S2) -> ZipLatest<S1, S2>
    where
        S1: Stream,
        S1::Item: Clone,
        S2: Stream<Error = S1::Error>,
        S2::Item: Clone
{
    ZipLatest {
        stream1: stream1.fuse(),
        stream2: stream2.fuse(),
        queued1: None,
        queued1_fresh: false,
        queued2: None,
        queued2_fresh: false,
    }
}

impl<S1, S2> Stream for ZipLatest<S1, S2>
    where S1: Stream,
          S1::Item: Clone,
          S2: Stream<Error = S1::Error>,
          S2::Item: Clone
{
    type Item = (S1::Item, S2::Item);
    type Error = S1::Error;

    fn poll_next(&mut self, cx: &mut task::Context)
        -> Poll<Option<Self::Item>, Self::Error>
    {
        self.queued1_fresh = enqueue(&mut self.stream1, &mut self.queued1,
            self.queued1_fresh, cx)?;
        self.queued2_fresh = enqueue(&mut self.stream2, &mut self.queued2,
            self.queued2_fresh, cx)?;
        let fresh = self.queued1_fresh || self.queued2_fresh;
        let done = self.stream1.is_done() && self.stream2.is_done()
            || self.stream1.is_done() && self.queued1.is_none()
            || self.stream2.is_done() && self.queued2.is_none();
        match (&self.queued1, &self.queued2) {
            (&Some(ref item1), &Some(ref item2)) if fresh => {
                self.queued1_fresh = false;
                self.queued2_fresh = false;
                Ok(Async::Ready(Some((item1.clone(), item2.clone()))))
            }
            _ if done => Ok(Async::Ready(None)),
            _ => Ok(Async::Pending),
        }
    }
}

fn enqueue<S: Stream>(stream: &mut S, queued: &mut Option<S::Item>,
    queued_fresh: bool, cx: &mut task::Context) -> Result<bool, S::Error>
{
    if queued.is_none() || !queued_fresh {
        match stream.poll_next(cx)? {
            Async::Ready(Some(item)) => {
                *queued = Some(item);
                Ok(true)
            }
            Async::Ready(None) | Async::Pending => Ok(queued_fresh),
        }
    } else {
        Ok(queued_fresh)
    }
}
