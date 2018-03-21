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

#[cfg(test)]
#[cfg(feature = "std")]
mod tests {
    use futures_core::{Async, Poll, Stream};
    use futures_executor::block_on;
    use std::boxed::Box;
    use std::iter::{Iterator, self};
    use std::vec::Vec;
    use stream::{self, StreamExt};

    #[test]
    fn zip_2_empty_streams() {
        let res = empty_stream::<(), ()>().zip_latest(empty_stream::<(), _>())
            .collect();
        assert_eq!(block_on(res), Ok(Vec::new()));
    }

    #[test]
    fn zip_empty_stream_and_other() {
        let res = empty_stream::<(), ()>().zip_latest(stream::iter_ok(0..3))
            .collect();
        assert_eq!(block_on(res), Ok(Vec::new()));
    }

    #[test]
    fn zip_other_and_empty_stream() {
        let res = stream::iter_ok(0..3).zip_latest(empty_stream::<(), ()>())
            .collect();
        assert_eq!(block_on(res), Ok(Vec::new()));
    }

    #[test]
    fn zip_same_length() {
        let res = stream::iter_ok::<_, ()>(0..3)
            .zip_latest(stream::iter_ok(0..3))
            .collect();
        let expected = (0..3).zip(0..3).collect::<Vec<_>>();
        assert_eq!(block_on(res), Ok(expected));
    }

    #[test]
    fn zip_long_short() {
        let res = stream::iter_ok::<_, ()>(0..3)
            .zip_latest(stream::iter_ok(0..2))
            .collect();
        let expected = vec![(0, 0), (1, 1), (2, 1)];
        assert_eq!(block_on(res), Ok(expected));
    }

    #[test]
    fn zip_short_long() {
        let res = stream::iter_ok::<_, ()>(0..2)
            .zip_latest(stream::iter_ok(0..3))
            .collect();
        let expected = vec![(0, 0), (1, 1), (1, 2)];
        assert_eq!(block_on(res), Ok(expected));
    }

    #[test]
    fn zip_with_error_left() {
        let res = stream::iter_result(vec![Ok(0), Ok(1), Err(()), Ok(2)])
            .zip_latest(stream::iter_ok::<_, ()>(0..3))
            .then(|x| Ok::<_, ()>(x.ok()))
            .filter_map(Ok)
            .collect();
        let expected = (0..3).zip(0..3).collect::<Vec<_>>();
        assert_eq!(block_on(res), Ok(expected));
    }

    #[test]
    fn zip_with_error_right() {
        let res = stream::iter_ok::<_, ()>(0..3)
            .zip_latest(stream::iter_result(vec![Ok(0), Ok(1), Err(()), Ok(2)]))
            .then(|x| Ok::<_, ()>(x.ok()))
            .filter_map(Ok)
            .collect();
        let expected = (0..3).zip(0..3).collect::<Vec<_>>();
        assert_eq!(block_on(res), Ok(expected));
    }

    #[test]
    fn zip_with_yield_left() {
        let values = vec![
            Ok::<_, ()>(Async::Ready(0)),
            Ok(Async::Ready(1)),
            Ok(Async::Pending),
            Ok(Async::Ready(2)),
        ];
        let other = stream::iter_ok::<_, ()>(0..3);
        let res = stream_from_poll_iter(values).zip_latest(other).collect();
        let expected = vec![(0, 0), (1, 1), (1, 2), (2, 2)];
        assert_eq!(block_on(res), Ok(expected));
    }

    #[test]
    fn zip_with_yield_right() {
        let values = vec![
            Ok::<_, ()>(Async::Ready(0)),
            Ok(Async::Ready(1)),
            Ok(Async::Pending),
            Ok(Async::Ready(2)),
        ];
        let other = stream_from_poll_iter(values);
        let res = stream::iter_ok::<_, ()>(0..3).zip_latest(other).collect();
        let expected = vec![(0, 0), (1, 1), (2, 1), (2, 2)];
        assert_eq!(block_on(res), Ok(expected));
    }

    fn empty_stream<T, E>() -> stream::IterOk<iter::Empty<T>, E> {
        stream::iter_ok(iter::empty())
    }

    fn stream_from_poll_iter<'a, I, T, E>(list: I)
        -> Box<Stream<Item = T, Error = E> + 'a>
        where
            I: IntoIterator<Item = Poll<T, E>>,
            I::IntoIter: 'a
    {
        let mut list = list.into_iter();
        let st = stream::poll_fn(move |cx| match list.next() {
            Some(Ok(Async::Ready(x))) => Ok(Async::Ready(Some(x))),
            Some(Ok(Async::Pending)) => {
                cx.waker().wake();
                Ok(Async::Pending)
            }
            Some(Err(e)) => Err(e),
            None => Ok(Async::Ready(None)),
        });
        Box::new(st)
    }
}
