use std::num::NonZeroUsize;
use std::sync::atomic::Ordering;

use crate::QueueAttr;
use crate::cacheline_aligned;
use crate::error::*;
use crate::shm::{Chunk, Span};

use crate::AtomicIndex;
use crate::Index;
use crate::MIN_MSGS;

const INVALID_INDEX: Index = Index::MAX;
const CONSUMED_FLAG: Index = Index::MAX - Index::MAX / 2;
const FIRST_FLAG: Index = CONSUMED_FLAG >> 1;

const ORIGIN_MASK: Index = CONSUMED_FLAG;

const INDEX_MASK: Index = !(ORIGIN_MASK | FIRST_FLAG);

#[derive(PartialEq, Eq)]
pub enum PopResult {
    /// An invalid index was written to shared memory (unrecoverable error).
    QueueError,

    /// No message has been produced yet.
    /// current_message will return None
    NoMessage,

    /// No new message has been produced, but an old one is still available.
    /// current_message will return old message
    NoNewMessage,

    /// A new message is available.
    Success,

    /// A new message is available, but one or more older messages were discarded by the producer.
    SuccessMessagesDiscarded,
}

#[derive(PartialEq, Eq)]
pub enum ForcePushResult {
    /// An invalid index was written to shared memory (unrecoverable error).
    QueueError,

    /// Message was successfully added.
    Success,

    /// Queue was full; message was added, but the oldest message was discarded.
    SuccessMessageDiscarded,
}

#[derive(PartialEq, Eq)]
pub enum TryPushResult {
    /// An invalid index was written to shared memory (unrecoverable error).
    QueueError,

    /// Queue was full; message was not added.
    QueueFull,

    /// Message was successfully added.
    Success,
}

struct Queue {
    _chunk: Chunk,
    message_size: NonZeroUsize,
    head: *mut Index,
    tail: *mut Index,
    chain: Vec<*mut Index>,
    messages: Vec<*mut ()>,
}

impl Queue {
    fn new(chunk: Chunk, attr: &QueueAttr) -> Result<Self, ShmMapError> {
        let queue_len = attr.additional_messages + MIN_MSGS;
        let index_size = size_of::<Index>();
        let queue_size = (2 + queue_len) * index_size;
        let message_size_aligned =
            NonZeroUsize::new(cacheline_aligned(attr.message_size.get())).unwrap();

        let mut offset_index = 0;
        let mut offset = cacheline_aligned(queue_size);

        let tail: *mut Index = chunk.get_ptr(offset_index)?;
        offset_index += index_size;

        let head: *mut Index = chunk.get_ptr(offset_index)?;
        offset_index += index_size;

        let mut chain: Vec<*mut Index> = Vec::with_capacity(queue_len);
        let mut messages: Vec<*mut ()> = Vec::with_capacity(queue_len);

        for _ in 0..queue_len {
            let index: *mut Index = chunk.get_ptr(offset_index)?;
            let message: *mut () = chunk.get_span_ptr(&Span {
                offset,
                size: message_size_aligned,
            })?;

            chain.push(index);
            messages.push(message);

            offset_index += index_size;
            offset += message_size_aligned.get();
        }

        Ok(Self {
            _chunk: chunk,
            message_size: attr.message_size,
            head,
            tail,
            chain,
            messages,
        })
    }

    fn is_valid_index(&self, idx: Index) -> bool {
        idx < self.len() as u32
    }

    fn init_shm(&self) {
        self.tail_store(INVALID_INDEX);
        self.head_store(INVALID_INDEX);
    }

    fn tail(&self) -> &AtomicIndex {
        unsafe { AtomicIndex::from_ptr(self.tail) }
    }

    fn head(&self) -> &AtomicIndex {
        unsafe { AtomicIndex::from_ptr(self.head) }
    }

    fn chain(&self, idx: Index) -> &AtomicIndex {
        unsafe { AtomicIndex::from_ptr(self.chain[idx as usize]) }
    }

    pub(self) fn tail_load(&self) -> Index {
        self.tail().load(Ordering::SeqCst)
    }

    pub(self) fn tail_store(&self, val: Index) {
        self.tail().store(val, Ordering::SeqCst)
    }

    pub(self) fn tail_fetch_or(&self, val: Index) -> Index {
        self.tail().fetch_or(val, Ordering::SeqCst)
    }

    pub(self) fn tail_compare_exchange(&self, current: Index, new: Index) -> bool {
        self.tail()
            .compare_exchange(current, new, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    pub(self) fn head_load(&self) -> Index {
        self.head().load(Ordering::SeqCst)
    }

    pub(self) fn head_store(&self, val: Index) {
        self.head().store(val, Ordering::SeqCst);
    }

    pub(self) fn chain_load(&self, idx: Index) -> Index {
        self.chain(idx).load(Ordering::SeqCst)
    }

    pub(self) fn queue_store(&self, idx: Index, val: Index) {
        self.chain(idx).store(val, Ordering::SeqCst);
    }

    pub(crate) fn len(&self) -> usize {
        self.chain.len()
    }
}

// every Queue has its own shared memory region
unsafe impl Send for Queue {}

pub struct ProducerQueue {
    queue: Queue,
    chain: Vec<Index>, /* local copy of queue, because queue is read only for consumer */
    head: Index, /* last message in chain that can be used by consumer, chain[head] is always INDEX_END */
    current: Index, /* message used by producer, will become head  */
    overrun: Index, /* message used by consumer when tail moved away by producer, will become current when released by consumer */
}

impl ProducerQueue {
    pub(crate) fn new(chunk: Chunk, attr: &QueueAttr) -> Result<Self, ShmMapError> {
        let queue = Queue::new(chunk, attr)?;
        let queue_len = queue.len();
        let mut chain: Vec<Index> = Vec::with_capacity(queue_len);
        let last = queue_len - 1;
        for i in 0..last {
            let next = i + 1;
            queue.queue_store(i as Index, next as Index);
            chain.push(next as Index);
        }

        queue.queue_store(last as Index, 0);
        chain.push(0);

        Ok(Self {
            queue,
            head: INVALID_INDEX,
            chain,
            current: 0,
            overrun: INVALID_INDEX,
        })
    }


    pub(crate) fn message_size(&self) -> NonZeroUsize {
        self.queue.message_size
    }

    pub(crate) fn init_shm(&self) {
        self.queue.init_shm();
    }

    pub(crate) fn current_message(&self) -> *mut () {
        let ptr = self.queue.messages.get(self.current as usize).unwrap();
        ptr.cast()
    }

    fn queue_store(&mut self, idx: Index, val: Index) {
        self.chain[idx as usize] = val;
        self.queue.queue_store(idx, val);
    }

    fn move_tail(&self, tail: Index) -> bool {
        let next = self.chain[(tail & INDEX_MASK) as usize];
        self.queue.tail_compare_exchange(tail, next)
    }

    fn enqueue_first_message(&mut self) {
        self.queue_store(self.current, INVALID_INDEX);

        self.queue.tail_store(self.current | FIRST_FLAG);

        self.head = self.current;

        self.queue.head_store(self.head);
    }

    fn enqueue_message(&mut self) {
        self.queue_store(self.current, INVALID_INDEX);

        self.queue_store(self.head, self.current);

        self.head = self.current;

        self.queue.head_store(self.head);
    }

    /* try to jump over tail blocked by consumer */
    fn overrun(&mut self, tail: Index) -> bool {
        let queue = &mut self.queue;

        let new_current = self.chain[(tail & INDEX_MASK) as usize]; /* next */
        let new_tail = self.chain[new_current as usize]; /* after next */

        if queue.tail_compare_exchange(tail, new_tail) {
            self.overrun = tail & INDEX_MASK;
            self.current = new_current;
            true
        } else {
            /* consumer just released tail, so use it */
            self.current = tail & INDEX_MASK;
            false
        }
    }

    pub(crate) fn full(&self) -> bool {
        if self.head == INVALID_INDEX {
            // queue is empty
            return false;
        }

        let tail = self.queue.tail_load();

        if !self.queue.is_valid_index(tail & INDEX_MASK) {
            // ERROR, queue is in invalid state, let producer move on and handle error on push
            return false;
        }

        if self.overrun != INVALID_INDEX {
            let consumed: bool = (tail & CONSUMED_FLAG) != 0;
            /* overrun mean the producer forced_push a message on a full queue
            queue has space if consumer moved on */
            !consumed
        } else {
            let next = self.chain[self.current as usize];
            let full: bool = next == (tail & INDEX_MASK);

            !full
        }
    }

    /* inserts the next message into the queue and
     * if the queue is full, discard the last message that is not
     * used by consumer. Returns pointer to new message */
    pub(crate) fn force_push(&mut self) -> ForcePushResult {
        let next = self.chain[self.current as usize];

        if self.head == INVALID_INDEX {
            self.enqueue_first_message();
            self.current = next;
            return ForcePushResult::Success;
        }

        let mut discarded = false;

        self.enqueue_message();

        let tail = self.queue.tail_load();

        if !self.queue.is_valid_index(tail & INDEX_MASK) {
            return ForcePushResult::QueueError;
        }

        let consumed: bool = (tail & CONSUMED_FLAG) != 0;

        if self.overrun != INVALID_INDEX {
            /* we overran the consumer and moved the tail, use overran message as
             * soon as the consumer releases it */
            if consumed {
                /* consumer released overrun message, so we can use it */
                /* requeue overrun */
                self.queue_store(self.overrun, next);

                self.current = self.overrun;
                self.overrun = INVALID_INDEX;
            } else {
                /* consumer still blocks overran message, move the tail again,
                 * because the message queue is still full */
                if self.move_tail(tail) {
                    self.current = tail & INDEX_MASK;
                    discarded = true;
                } else {
                    /* consumer just released overrun message, so we can use it */
                    /* requeue overrun */
                    self.queue_store(self.overrun, next);

                    self.current = self.overrun;
                    self.overrun = INVALID_INDEX;
                }
            }
        } else {
            let full: bool = next == (tail & INDEX_MASK);

            /* no previous overrun, use next or after next message */
            if !full {
                /* message queue not full, simply use next */
                self.current = next;
            } else if !consumed {
                /* message queue is full, but no message is consumed yet, so try to move tail */
                if self.move_tail(tail) {
                    /* message queue is full -> tail & INDEX_MASK == next */
                    self.current = next;
                    discarded = true;
                } else {
                    /*  consumer just started and consumed tail
                     *  we're assuming that consumer flagged tail (tail | CONSUMED_FLAG),
                     *  if this this is not the case, consumer already moved on
                     *  and we will use tail  */
                    discarded = self.overrun(tail | CONSUMED_FLAG);
                }
            } else {
                /* overrun the consumer, if the consumer keeps tail */
                discarded = self.overrun(tail);
            }
        }

        if discarded {
            ForcePushResult::SuccessMessageDiscarded
        } else {
            ForcePushResult::Success
        }
    }

    /* trys to insert the next message into the queue */
    pub(crate) fn try_push(&mut self) -> TryPushResult {
        let next = self.chain[self.current as usize];

        if self.head == INVALID_INDEX {
            self.enqueue_first_message();
            self.current = next;
            return TryPushResult::Success;
        }

        let tail = self.queue.tail_load();

        if !self.queue.is_valid_index(tail & INDEX_MASK) {
            return TryPushResult::QueueError;
        }

        if self.overrun != INVALID_INDEX {
            let consumed = (tail & CONSUMED_FLAG) != 0;

            if consumed {
                /* consumer released overrun message, so we can use it */
                /* requeue overrun */
                self.enqueue_message();

                self.queue_store(self.overrun, next);

                self.current = self.overrun;
                self.overrun = INVALID_INDEX;
                return TryPushResult::Success;
            }
        } else {
            let full = next == (tail & INDEX_MASK);

            /* no previous overrun, use next or after next message */
            if !full {
                self.enqueue_message();
                self.current = next;
                return TryPushResult::Success;
            }
        }
        TryPushResult::QueueFull
    }
}

pub struct ConsumerQueue {
    queue: Queue,
    current: Index,
}

impl ConsumerQueue {
    pub(crate) fn new(chunk: Chunk, attr: &QueueAttr) -> Result<Self, ShmMapError> {
        let queue = Queue::new(chunk, attr)?;
        Ok(Self { queue, current: 0 })
    }

    pub(crate) fn current_message(&self) -> Option<*const ()> {
        let ptr = self.queue.messages.get(self.current as usize)?;
        Some(ptr.cast())
    }

    pub(crate) fn message_size(&self) -> NonZeroUsize {
        self.queue.message_size
    }

    pub(crate) fn init_shm(&self) {
        self.queue.init_shm();
    }

    pub(crate) fn flush(&mut self) -> PopResult {
        loop {
            let tail = self.queue.tail_fetch_or(CONSUMED_FLAG);

            if tail == INVALID_INDEX {
                /* or CONSUMED_FLAG doesn't change INDEX_END*/
                return PopResult::NoMessage;
            }

            if !self.queue.is_valid_index(tail & INDEX_MASK) {
                return PopResult::QueueError;
            }

            let head = self.queue.head_load();

            if !self.queue.is_valid_index(head) {
                return PopResult::QueueError;
            }

            if self
                .queue
                .tail_compare_exchange(tail | CONSUMED_FLAG, head | CONSUMED_FLAG)
            {
                /* only accept head if producer didn't move tail,
                 *  otherwise the producer could fill the whole queue and the head could be the
                 *  producers current message  */
                self.current = head;
                return PopResult::Success;
            }
        }
    }

    pub(crate) fn pop(&mut self) -> PopResult {
        let tail = self.queue.tail_fetch_or(CONSUMED_FLAG);

        if tail == INVALID_INDEX {
            return PopResult::NoMessage;
        }

        if !self.queue.is_valid_index(tail & INDEX_MASK) {
            return PopResult::QueueError;
        }

        if tail & CONSUMED_FLAG == 0 {
            /* producer moved tail, use it */
            self.current = tail & INDEX_MASK;
            if (tail & FIRST_FLAG) == FIRST_FLAG {
                return PopResult::Success;
            } else {
                return PopResult::SuccessMessagesDiscarded;
            }
        }

        /* try to get next message */
        let next = self.queue.chain_load(self.current);

        if next == INVALID_INDEX {
            return PopResult::NoNewMessage;
        }

        if !self.queue.is_valid_index(next) {
            return PopResult::QueueError;
        }

        if self.queue.tail_compare_exchange(tail, next | CONSUMED_FLAG) {
            self.current = next;
            PopResult::Success
        } else {
            /* producer just moved tail, use it */
            let current = self.queue.tail_fetch_or(CONSUMED_FLAG);

            if !self.queue.is_valid_index(current) {
                return PopResult::QueueError;
            }

            self.current = current;
            PopResult::SuccessMessagesDiscarded
        }
    }
}
