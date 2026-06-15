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
    ChannelConfig, QueueConfig, VectorConfig,
    error::*,
    protocol::{create_request, parse_request},
    queue::{ConsumerQueue, ForcePushResult, PopResult, ProducerQueue, Queue, TryPushResult},
    shm::SharedMemory,
    unix::{check_memfd, eventfd_create, into_eventfd, shmfd_create},
};

pub struct Producer<T: Copy> {
    queue: ProducerQueue,
    eventfd: Option<EventFd>,
    cache: Option<Box<T>>,
    _type: PhantomData<T>,
}

impl<T: Copy> Producer<T> {
    fn new(channel: Channel) -> Result<Self, ShmMapError> {
        if size_of::<T>() > channel.queue.message_size().get() {
            return Err(ShmMapError::OutOfBounds);
        }

        let queue = ProducerQueue::new(channel.queue);

        Ok(Self {
            queue,
            eventfd: channel.eventfd,
            cache: None,
            _type: PhantomData,
        })
    }

    pub fn current_message(&mut self) -> &mut T {
        if let Some(ref mut cache) = self.cache {
            cache.borrow_mut()
        } else {
            unsafe { &mut *self.queue.current_message().cast::<T>() }
        }
    }

    pub fn force_push(&mut self) -> ForcePushResult {
        if let Some(ref cache) = self.cache {
            *self.current_message() = *cache.clone();
        }

        let result = self.queue.force_push();

        if result == ForcePushResult::Success {
            self.eventfd.as_ref().map(|fd| fd.write(1));
        }

        result
    }

    pub fn try_push(&mut self) -> TryPushResult {
        if let Some(ref cache) = self.cache {
            if self.queue.full() {
                return TryPushResult::QueueFull;
            }
            *self.current_message() = *cache.clone();
        }

        let result = self.queue.try_push();
        if result == TryPushResult::Success {
            self.eventfd.as_ref().map(|fd| fd.write(1));
        }
        result
    }

    pub fn eventfd(&self) -> Option<BorrowedFd<'_>> {
        self.eventfd.as_ref().map(|fd| fd.as_fd())
    }

    pub fn take_eventfd(&mut self) -> Option<EventFd> {
        self.eventfd.take()
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
    queue: ConsumerQueue,
    eventfd: Option<EventFd>,
    _type: PhantomData<T>,
}

impl<T: Copy> Consumer<T> {
    fn new(channel: Channel) -> Result<Self, ShmMapError> {
        if size_of::<T>() > channel.queue.message_size().get() {
            return Err(ShmMapError::OutOfBounds);
        }

        let queue = ConsumerQueue::new(channel.queue);

        Ok(Self {
            queue,
            eventfd: channel.eventfd,
            _type: PhantomData,
        })
    }

    pub fn current_message(&self) -> Option<&T> {
        let ptr: *const T = self.queue.current_message()?.cast();
        Some(unsafe { &*ptr })
    }

    pub fn pop(&mut self) -> PopResult {
        if let Some(eventfd) = self.eventfd.as_ref()
            && eventfd.read().is_err()
        {
            if self.queue.current_message().is_some() {
                return PopResult::NoNewMessage;
            } else {
                return PopResult::NoMessage;
            }
        }

        self.queue.pop()
    }

    pub fn flush(&mut self) -> PopResult {
        if self.eventfd.is_some() {
            let mut result = PopResult::NoMessage;
            while self.pop() == PopResult::Success {
                result = PopResult::Success;
            }
            result
        } else {
            self.queue.flush()
        }
    }

    pub fn eventfd(&self) -> Option<BorrowedFd<'_>> {
        self.eventfd.as_ref().map(|fd| fd.as_fd())
    }

    pub fn take_eventfd(&mut self) -> Option<EventFd> {
        self.eventfd.take()
    }
}

pub(crate) struct Channel {
    queue: Queue,
    info: Vec<u8>,
    eventfd: Option<EventFd>,
}

impl Channel {
    pub fn allocate(
        config: &ChannelConfig,
        shm: &SharedMemory,
        shm_offset: &mut usize,
    ) -> Result<Self, ResourceError> {
        let eventfd = if config.eventfd {
            let eventfd = eventfd_create()?;
            Some(eventfd)
        } else {
            None
        };
        let channel = Self::new(&config.queue, eventfd, &config.info, shm, shm_offset)?;
        channel.queue.init();
        Ok(channel)
    }

    pub fn new(
        config: &QueueConfig,
        eventfd: Option<EventFd>,
        info: &[u8],
        shm: &SharedMemory,
        shm_offset: &mut usize,
    ) -> Result<Self, ResourceError> {
        let shm_size = config.shm_size();
        let chunk = shm.alloc(*shm_offset, shm_size)?;
        let queue = Queue::new(chunk, config)?;

        *shm_offset += shm_size.get();

        Ok(Channel {
            queue,
            info: info.to_vec(),
            eventfd,
        })
    }
    pub fn config(&self) -> ChannelConfig {
        ChannelConfig {
            queue: self.queue.config(),
            eventfd: self.eventfd.is_some(),
            info: self.info.clone(),
        }
    }
}

pub struct ChannelVector {
    shm: Arc<SharedMemory>,
    producers: Vec<Option<Channel>>,
    consumers: Vec<Option<Channel>>,
    info: Vec<u8>,
}

impl ChannelVector {
    pub fn new(vconfig: &VectorConfig) -> Result<Self, ResourceError> {
        let mut producers = Vec::<Option<Channel>>::with_capacity(vconfig.producers.len());
        let mut consumers = Vec::<Option<Channel>>::with_capacity(vconfig.consumers.len());

        let shm_size =
            NonZeroUsize::new(vconfig.calc_shm_size()).ok_or(ResourceError::InvalidArgument)?;

        let shmfd = shmfd_create(shm_size)?;

        let shm = SharedMemory::new(shmfd)?;

        let mut shm_offset = 0;

        for config in &vconfig.producers {
            let channel = Channel::allocate(config, &shm, &mut shm_offset)?;

            producers.push(Some(channel));
        }

        for config in &vconfig.consumers {
            let channel = Channel::allocate(config, &shm, &mut shm_offset)?;

            consumers.push(Some(channel));
        }

        Ok(Self {
            shm,
            consumers,
            producers,
            info: vconfig.info.clone(),
        })
    }

    pub fn consumer_info(&self, index: usize) -> Option<&Vec<u8>> {
        self.consumers.get(index)?.as_ref().map(|c| &c.info)
    }

    pub fn producer_info(&self, index: usize) -> Option<&Vec<u8>> {
        self.producers.get(index)?.as_ref().map(|c| &c.info)
    }

    pub fn take_consumer<T: Copy>(&mut self, index: usize) -> Option<Consumer<T>> {
        let channel = self.consumers.get_mut(index)?.take()?;
        let consumer = Consumer::new(channel).ok()?;
        Some(consumer)
    }

    pub fn take_producer<T: Copy>(&mut self, index: usize) -> Option<Producer<T>> {
        let channel = self.producers.get_mut(index)?.take()?;
        let producer = Producer::new(channel).ok()?;
        Some(producer)
    }

    pub fn info(&self) -> &Vec<u8> {
        &self.info
    }

    pub fn config(&self) -> VectorConfig {
        let producers = self
            .producers
            .iter()
            .flatten()
            .map(|c| c.config())
            .collect();
        let consumers = self
            .consumers
            .iter()
            .flatten()
            .map(|c| c.config())
            .collect();
        VectorConfig {
            producers,
            consumers,
            info: self.info.clone(),
        }
    }

    fn collect_eventfds(&self) -> Vec<BorrowedFd<'_>> {
        let producer_eventfds: Vec<BorrowedFd<'_>> = self
            .consumers
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
        let vconfig = self.config();
        let req = create_request(&vconfig);
        (req, self.collect_eventfds())
    }

    pub fn deserialize(request: &[u8], mut fds: VecDeque<OwnedFd>) -> Result<Self, TransferError> {
        let vconfig = parse_request(request)?;

        let mut producers = Vec::<Option<Channel>>::with_capacity(vconfig.producers.len());
        let mut consumers = Vec::<Option<Channel>>::with_capacity(vconfig.consumers.len());

        let shmfd = fds
            .pop_front()
            .ok_or(TransferError::MissingFileDescriptor)?;

        let n_consumer_fds = vconfig.count_consumer_eventfds();

        let mut producer_fds = fds.split_off(n_consumer_fds);
        let mut consumer_fds = fds;

        check_memfd(shmfd.as_fd())?;

        let shm = SharedMemory::new(shmfd)?;

        let mut shm_offset = 0;

        for config in &vconfig.consumers {
            let eventfd = if config.eventfd {
                let fd = consumer_fds
                    .pop_front()
                    .ok_or(TransferError::MissingFileDescriptor)?;
                let eventfd = into_eventfd(fd)?;
                Some(eventfd)
            } else {
                None
            };
            let channel =
                Channel::new(&config.queue, eventfd, &config.info, &shm, &mut shm_offset)?;

            consumers.push(Some(channel));
        }

        for config in &vconfig.producers {
            let eventfd = if config.eventfd {
                let fd = producer_fds
                    .pop_front()
                    .ok_or(TransferError::MissingFileDescriptor)?;
                let eventfd = into_eventfd(fd)?;
                Some(eventfd)
            } else {
                None
            };
            let channel =
                Channel::new(&config.queue, eventfd, &config.info, &shm, &mut shm_offset)?;

            producers.push(Some(channel));
        }

        Ok(Self {
            shm,
            consumers,
            producers,
            info: vconfig.info.clone(),
        })
    }
}
