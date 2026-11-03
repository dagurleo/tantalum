//! Waking a connection's task from whichever thread has news for it: a
//! write that serves a blocked client, a message for a subscriber.

use std::sync::Arc;
use std::task::Waker;

use parking_lot::Mutex;

/// A connection's waker. A monoio connection can only be woken from its own
/// thread, so it comes with that thread's mailbox.
pub(crate) struct Wake {
    waker: Waker,
    home: Option<Arc<Mailbox>>,
}

impl Wake {
    pub(crate) fn new(cx: &std::task::Context<'_>, home: Option<&Arc<Mailbox>>) -> Self {
        Wake {
            waker: cx.waker().clone(),
            home: home.cloned(),
        }
    }

    pub(crate) fn wake(self) {
        match self.home {
            Some(home) => home.send(self.waker),
            None => self.waker.wake(),
        }
    }
}

/// Wakes a monoio thread's tasks from other threads, which monoio itself
/// can't do (without its `sync` feature, which costs every connection).
/// The thread reads `bell` and wakes what was sent.
pub struct Mailbox {
    wakers: Mutex<Vec<Waker>>,
    bell: std::os::unix::net::UnixStream,
}

impl Mailbox {
    /// A mailbox, and the other end of its bell, for its thread to read.
    pub fn new() -> std::io::Result<(Arc<Mailbox>, std::os::unix::net::UnixStream)> {
        let (bell, ear) = std::os::unix::net::UnixStream::pair()?;
        bell.set_nonblocking(true)?;
        ear.set_nonblocking(true)?;
        let mailbox = Mailbox {
            wakers: Mutex::new(Vec::new()),
            bell,
        };
        Ok((Arc::new(mailbox), ear))
    }

    fn send(&self, waker: Waker) {
        let first = {
            let mut wakers = self.wakers.lock();
            wakers.push(waker);
            wakers.len() == 1
        };
        // Only the first rings: the rest come along with it. (If the bell's
        // buffer is full, it's ringing already.)
        if first {
            let _ = std::io::Write::write(&mut &self.bell, &[1]);
        }
    }

    /// On its own thread, after the bell rang: wakes what was sent.
    pub fn deliver(&self) {
        let wakers = std::mem::take(&mut *self.wakers.lock());
        wakers.into_iter().for_each(Waker::wake);
    }
}
