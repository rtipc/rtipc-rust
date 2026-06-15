#![cfg(unix)]

use std::{
    fmt,
    mem::size_of,
    num::NonZeroUsize,
    os::fd::{AsFd, BorrowedFd, OwnedFd},
    ptr::NonNull,
    sync::{Arc, Weak},
};

use nix::{
    errno::Errno,
    libc::c_void,
    sys::{
        mman::{MapFlags, ProtFlags, mlock, mmap, munmap},
        stat::fstat,
    },
};

use crate::error::*;
use crate::log::*;

#[derive(Debug, Copy, Clone)]
pub(crate) struct Span {
    pub offset: usize,
    pub size: NonZeroUsize,
}

pub(crate) struct Chunk {
    shm: Arc<SharedMemory>,
    offset: usize,
    size: NonZeroUsize,
}

impl Chunk {
    pub(crate) fn get_ptr<T>(&self, offset: usize) -> Result<*mut T, ShmMapError> {
        let size = NonZeroUsize::new(size_of::<T>()).unwrap();
        let ptr = self.get_span_ptr(&Span { offset, size })?;

        Ok(ptr.cast())
    }

    pub(crate) fn get_span_ptr(&self, span: &Span) -> Result<*mut (), ShmMapError> {
        if span.offset + span.size.get() > self.size.get() {
            return Err(ShmMapError::OutOfBounds);
        }

        let ptr: *mut () = unsafe { self.shm.ptr.byte_add(self.offset + span.offset) };

        Ok(ptr)
    }
}

#[derive(Debug)]
pub struct SharedMemory {
    me: Weak<Self>,
    fd: OwnedFd,
    ptr: *mut (),
    size: NonZeroUsize,
}

impl SharedMemory {
    pub fn alloc(&self, offset: usize, size: NonZeroUsize) -> Result<Chunk, ShmMapError> {
        if offset + size.get() > self.size.get() {
            return Err(ShmMapError::OutOfBounds);
        }

        Ok(Chunk {
            shm: self.me.upgrade().unwrap(),
            offset,
            size,
        })
    }

    pub fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }

    pub fn new(fd: OwnedFd) -> Result<Arc<Self>, Errno> {
        let stat = fstat(&fd)?;

        let size = NonZeroUsize::new(stat.st_size as usize).ok_or(Errno::EBADFD)?;

        let ptr = unsafe {
            mmap(
                None,                                         // Desired addr
                size,                                         // size of mapping
                ProtFlags::PROT_READ | ProtFlags::PROT_WRITE, // Permissions on pages
                MapFlags::MAP_SHARED,                         // What kind of mapping
                &fd,                                          // fd
                0,                                            // Offset into fd
            )
        }?;

        unsafe {
            mlock(ptr, size.get())?;
        }

        Ok(Arc::new_cyclic(|me| Self {
            me: me.clone(),
            fd,
            ptr: ptr.as_ptr().cast(),
            size,
        }))
    }
}

impl Drop for SharedMemory {
    fn drop(&mut self) {
        let ptr: NonNull<c_void> = NonNull::new(self.ptr as *mut c_void).unwrap();
        debug!("unmap {ptr:?}");
        if let Err(_e) = unsafe { munmap(ptr, self.size.get()) } {
            error!("munmap failed with : {_e}");
        }
    }
}

impl fmt::Display for SharedMemory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ptr: {:p}, size: {}", self.ptr, self.size)
    }
}
