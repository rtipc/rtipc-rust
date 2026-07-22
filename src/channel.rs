use std::{
    borrow::BorrowMut,
    collections::VecDeque,
    marker::PhantomData,
    mem::size_of,
    num::NonZeroUsize,
    os::fd::{AsFd, BorrowedFd, OwnedFd},
    sync::Arc,
};

use nix::sys::eventfd::EventFd;

use crate::{
    ChannelAttr, GroupAttr, QueueAttr,
    error::*,
    protocol::{create_request, parse_request},
    queue::{ConsumerQueue, ForcePushResult, PopResult, ProducerQueue, TryPushResult},
    shm::SharedMemory,
    unix::{check_memfd, eventfd_create, into_eventfd, shmfd_create},
};

pub struct Producer<T: Copy> {
    channel: ProducerChannel,
    cache: Option<Box<T>>,
    _type: PhantomData<T>,
}

impl<T: Copy> Producer<T> {
    fn new(channel: ProducerChannel) -> Result<Self, ShmMapError> {
        if size_of::<T>() > channel.queue.message_size().get() {
            return Err(ShmMapError::OutOfBounds);
        }

        Ok(Self {
            channel,
            cache: None,
            _type: PhantomData,
        })
    }

    pub fn current_message(&mut self) -> &mut T {
        if let Some(ref mut cache) = self.cache {
            cache.borrow_mut()
        } else {
            unsafe { &mut *self.channel.queue.current_message().cast::<T>() }
        }
    }

    pub fn force_push(&mut self) -> ForcePushResult {
        if let Some(ref cache) = self.cache {
            *self.current_message() = *cache.clone();
        }

        let result = self.channel.queue.force_push();

        if result == ForcePushResult::Success {
            self.channel.eventfd.as_ref().map(|fd| fd.write(1));
        }

        result
    }

    pub fn try_push(&mut self) -> TryPushResult {
        if let Some(ref cache) = self.cache {
            if self.channel.queue.full() {
                return TryPushResult::QueueFull;
            }
            *self.current_message() = *cache.clone();
        }

        let result = self.channel.queue.try_push();
        if result == TryPushResult::Success {
            self.channel.eventfd.as_ref().map(|fd| fd.write(1));
        }
        result
    }

    pub fn eventfd(&self) -> Option<BorrowedFd<'_>> {
        self.channel.eventfd.as_ref().map(|fd| fd.as_fd())
    }

    pub fn take_eventfd(&mut self) -> Option<EventFd> {
        self.channel.eventfd.take()
    }

    pub fn enable_cache(&mut self) {
        if self.cache.is_none() {
            self.cache = Some(Box::new(*self.current_message()));
        }
    }

    pub fn disable_cache(&mut self) {
        if let Some(cache) = self.cache.take() {
            *self.current_message() = *cache;
        }
    }
}

pub struct Consumer<T: Copy> {
    channel: ConsumerChannel,
    _type: PhantomData<T>,
}

impl<T: Copy> Consumer<T> {
    fn new(channel: ConsumerChannel) -> Result<Self, ShmMapError> {
        if size_of::<T>() > channel.queue.message_size().get() {
            return Err(ShmMapError::OutOfBounds);
        }

        Ok(Self {
            channel,
            _type: PhantomData,
        })
    }

    pub fn current_message(&self) -> Option<&T> {
        let ptr: *const T = self.channel.queue.current_message()?.cast();
        Some(unsafe { &*ptr })
    }

    pub fn pop(&mut self) -> PopResult {
        if let Some(eventfd) = self.channel.eventfd.as_ref()
            && eventfd.read().is_err()
        {
            if self.channel.queue.current_message().is_some() {
                return PopResult::NoNewMessage;
            } else {
                return PopResult::NoMessage;
            }
        }

        self.channel.queue.pop()
    }

    pub fn flush(&mut self) -> PopResult {
        if self.channel.eventfd.is_some() {
            let mut result = PopResult::NoMessage;
            while self.pop() == PopResult::Success {
                result = PopResult::Success;
            }
            result
        } else {
            self.channel.queue.flush()
        }
    }

    pub fn eventfd(&self) -> Option<BorrowedFd<'_>> {
        self.channel.eventfd.as_ref().map(|fd| fd.as_fd())
    }

    pub fn take_eventfd(&mut self) -> Option<EventFd> {
        self.channel.eventfd.take()
    }
}

pub(crate) struct ConsumerChannel {
    queue: ConsumerQueue,
    eventfd: Option<EventFd>,
}

impl ConsumerChannel {
    pub fn allocate(
        attr: &ChannelAttr,
        shm: &SharedMemory,
        shm_offset: &mut usize,
    ) -> Result<Self, ResourceError> {
        let eventfd = if attr.eventfd {
            let eventfd = eventfd_create()?;
            Some(eventfd)
        } else {
            None
        };
        let channel = Self::new(&attr.to_queue_attr(), eventfd, shm, shm_offset)?;
        channel.queue.init_shm();
        Ok(channel)
    }

    pub fn new(
        attr: &QueueAttr,
        eventfd: Option<EventFd>,
        shm: &SharedMemory,
        shm_offset: &mut usize,
    ) -> Result<Self, ResourceError> {
        let shm_size = attr.shm_size();
        let chunk = shm.alloc(*shm_offset, shm_size)?;
        let queue = ConsumerQueue::new(chunk, attr)?;

        *shm_offset += shm_size.get();

        Ok(Self { queue, eventfd })
    }
}

pub(crate) struct ProducerChannel {
    queue: ProducerQueue,
    eventfd: Option<EventFd>,
}

impl ProducerChannel {
    pub fn allocate(
        attr: &ChannelAttr,
        shm: &SharedMemory,
        shm_offset: &mut usize,
    ) -> Result<Self, ResourceError> {
        let eventfd = if attr.eventfd {
            let eventfd = eventfd_create()?;
            Some(eventfd)
        } else {
            None
        };
        let channel = Self::new(&attr.to_queue_attr(), eventfd, shm, shm_offset)?;
        channel.queue.init_shm();
        Ok(channel)
    }

    pub fn new(
        attr: &QueueAttr,
        eventfd: Option<EventFd>,
        shm: &SharedMemory,
        shm_offset: &mut usize,
    ) -> Result<Self, ResourceError> {
        let shm_size = attr.shm_size();
        let chunk = shm.alloc(*shm_offset, shm_size)?;
        let queue = ProducerQueue::new(chunk, attr)?;

        *shm_offset += shm_size.get();

        Ok(Self { queue, eventfd })
    }
}

pub struct ChannelGroup {
    attr: GroupAttr,
    shm: Arc<SharedMemory>,
    producers: Vec<Option<ProducerChannel>>,
    consumers: Vec<Option<ConsumerChannel>>,
}

impl ChannelGroup {
    pub fn from_attr(attr: &GroupAttr) -> Result<Self, ResourceError> {
        let mut producers = Vec::<Option<ProducerChannel>>::with_capacity(attr.producers.len());
        let mut consumers = Vec::<Option<ConsumerChannel>>::with_capacity(attr.consumers.len());

        let shm_size =
            NonZeroUsize::new(attr.calc_shm_size()).ok_or(ResourceError::InvalidArgument)?;

        let shmfd = shmfd_create(shm_size)?;

        let shm = SharedMemory::new(shmfd)?;

        let mut shm_offset = 0;

        for attr in &attr.producers {
            let channel = ProducerChannel::allocate(attr, &shm, &mut shm_offset)?;

            producers.push(Some(channel));
        }

        for attr in &attr.consumers {
            let channel = ConsumerChannel::allocate(attr, &shm, &mut shm_offset)?;

            consumers.push(Some(channel));
        }

        Ok(Self {
            attr: attr.clone(),
            shm,
            consumers,
            producers,
        })
    }

    pub fn acquire_consumer<T: Copy>(&mut self, index: usize) -> Option<Consumer<T>> {
        let channel = self.consumers.get_mut(index)?.take()?;
        let consumer = Consumer::new(channel).ok()?;
        Some(consumer)
    }

    pub fn acquire_producer<T: Copy>(&mut self, index: usize) -> Option<Producer<T>> {
        let channel = self.producers.get_mut(index)?.take()?;
        let producer = Producer::new(channel).ok()?;
        Some(producer)
    }

    pub fn get_attr(&self) -> &GroupAttr {
        &self.attr
    }

    fn collect_eventfds(&self) -> Vec<BorrowedFd<'_>> {
        let producer_eventfds: Vec<BorrowedFd<'_>> = self
            .producers
            .iter()
            .flatten()
            .filter_map(|c| c.eventfd.as_ref().map(|fd| fd.as_fd()))
            .collect();
        let consumer_eventfds: Vec<BorrowedFd<'_>> = self
            .consumers
            .iter()
            .flatten()
            .filter_map(|c| c.eventfd.as_ref().map(|fd| fd.as_fd()))
            .collect();

        [vec![self.shm.as_fd()], producer_eventfds, consumer_eventfds].concat()
    }

    pub fn serialize(&self) -> (Vec<u8>, Vec<BorrowedFd<'_>>) {
        let req = create_request(&self.attr);
        (req, self.collect_eventfds())
    }

    pub fn deserialize(request: &[u8], mut fds: VecDeque<OwnedFd>) -> Result<Self, TransferError> {
        let attr = parse_request(request)?;

        let mut producers = Vec::<Option<ProducerChannel>>::with_capacity(attr.producers.len());
        let mut consumers = Vec::<Option<ConsumerChannel>>::with_capacity(attr.consumers.len());

        let shmfd = fds
            .pop_front()
            .ok_or(TransferError::MissingFileDescriptor)?;

        let n_consumer_fds = attr.count_consumer_eventfds();

        let mut producer_fds = fds.split_off(n_consumer_fds);
        let mut consumer_fds = fds;

        check_memfd(shmfd.as_fd())?;

        let shm = SharedMemory::new(shmfd)?;

        let mut shm_offset = 0;

        for attr in &attr.consumers {
            let eventfd = if attr.eventfd {
                let fd = consumer_fds
                    .pop_front()
                    .ok_or(TransferError::MissingFileDescriptor)?;
                let eventfd = into_eventfd(fd)?;
                Some(eventfd)
            } else {
                None
            };
            let channel =
                ConsumerChannel::new(&attr.to_queue_attr(), eventfd, &shm, &mut shm_offset)?;

            consumers.push(Some(channel));
        }

        for attr in &attr.producers {
            let eventfd = if attr.eventfd {
                let fd = producer_fds
                    .pop_front()
                    .ok_or(TransferError::MissingFileDescriptor)?;
                let eventfd = into_eventfd(fd)?;
                Some(eventfd)
            } else {
                None
            };
            let channel =
                ProducerChannel::new(&attr.to_queue_attr(), eventfd, &shm, &mut shm_offset)?;

            producers.push(Some(channel));
        }

        Ok(Self {
            attr,
            shm,
            consumers,
            producers,
        })
    }
}
