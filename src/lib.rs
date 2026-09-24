#[cfg(feature = "predefined_cacheline_size")]
mod cache_env;
#[cfg(not(feature = "predefined_cacheline_size"))]
mod cache_linux;
mod channel;
pub mod error;
mod header;
mod protocol;
mod queue;
mod shm;
mod socket;
mod unix;

#[macro_use]
extern crate nix;

use std::{num::NonZeroUsize, sync::atomic::AtomicU32};

#[cfg(feature = "predefined_cacheline_size")]
pub use crate::cache_env::max_cacheline_size;

#[cfg(not(feature = "predefined_cacheline_size"))]
pub use crate::cache_linux::max_cacheline_size;

pub use channel::{ChannelGroup, Consumer, Producer};
pub use error::*;
pub use queue::{ForcePushResult, PopResult, TryPushResult};
pub use socket::{Server, client_connect, client_connect_fd};

pub use nix::errno::Errno;
pub use nix::sys::eventfd::EventFd;

pub use log;

pub(crate) type AtomicIndex = AtomicU32;
pub(crate) type Index = u32;
pub(crate) const MIN_MSGS: usize = 3;

pub fn index_size() -> usize {
    std::mem::size_of::<Index>()
}

pub(crate) fn mem_align(size: usize, alignment: usize) -> usize {
    (size + alignment - 1) & !(alignment - 1)
}

pub(crate) fn cacheline_aligned(size: usize) -> usize {
    mem_align(size, max_cacheline_size())
}

#[derive(Clone)]
pub struct QueueAttr {
    pub additional_messages: usize,
    pub message_size: NonZeroUsize,
}

impl QueueAttr {
    fn data_size(&self) -> usize {
        let n = MIN_MSGS + self.additional_messages;

        n * cacheline_aligned(self.message_size.get())
    }

    fn queue_size(&self) -> usize {
        let n = 2 + MIN_MSGS + self.additional_messages;
        cacheline_aligned(n * std::mem::size_of::<Index>())
    }

    pub(crate) fn shm_size(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.queue_size() + self.data_size()).unwrap()
    }
}

#[derive(Eq, Clone)]
pub struct ChannelAttr {
    pub additional_messages: usize,
    pub message_size: NonZeroUsize,
    pub eventfd: bool,
    pub info: Vec<u8>,
}

impl ChannelAttr {
    fn to_queue_attr(&self) -> QueueAttr {
        QueueAttr {
            additional_messages: self.additional_messages,
            message_size: self.message_size,
        }
    }
}

impl PartialEq for ChannelAttr {
    fn eq(&self, other: &Self) -> bool {
        self.additional_messages == other.additional_messages
            && self.message_size == other.message_size
            && self.eventfd == other.eventfd
            && self.info == other.info
    }
}

#[derive(Eq, Clone)]
pub struct GroupAttr {
    pub producers: Vec<ChannelAttr>,
    pub consumers: Vec<ChannelAttr>,
    pub info: Vec<u8>,
}

impl GroupAttr {
    pub fn count_producer_eventfds(&self) -> usize {
        self.producers.iter().map(|c| c.eventfd as usize).sum()
    }

    pub fn count_consumer_eventfds(&self) -> usize {
        self.consumers.iter().map(|c| c.eventfd as usize).sum()
    }

    pub fn calc_shm_size(&self) -> usize {
        let producers_size: usize = self
            .producers
            .iter()
            .map(|c| c.to_queue_attr().shm_size().get())
            .sum();

        let consumers_size: usize = self
            .consumers
            .iter()
            .map(|c| c.to_queue_attr().shm_size().get())
            .sum();

        producers_size + consumers_size
    }
}

impl PartialEq for GroupAttr {
    fn eq(&self, other: &Self) -> bool {
        self.consumers == other.consumers
            && self.producers == other.producers
            && self.info == other.info
    }
}
