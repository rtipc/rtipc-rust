use nix::errno::Errno;

#[derive(Debug)]
pub enum ShmMapError {
    OutOfBounds,
    Misalignment,
}

#[derive(Debug)]
pub enum HeaderError {
    SizeExceedsRequest,
    MagicMismatch,
    VersionMismatch,
    CachelineSizeMismatch,
    AtomicSizeMismatch,
}

#[derive(Debug)]
pub enum ResourceError {
    InvalidArgument,
    Errno(Errno),
    ShmMapError(ShmMapError),
}

#[derive(Debug)]
pub enum RequestError {
    OutOfBounds,
    HeaderError(HeaderError),
}

#[derive(Debug)]
pub enum QueueError {
    InvalidIndex,
}

#[derive(Debug)]
pub enum TransferError {
    ResourceError(ResourceError),
    RequestError(RequestError),
    MissingFileDescriptor,
    Rejected,
    ResponseError,
}

impl From<Errno> for ResourceError {
    fn from(e: Errno) -> ResourceError {
        ResourceError::Errno(e)
    }
}

impl From<ShmMapError> for ResourceError {
    fn from(e: ShmMapError) -> ResourceError {
        ResourceError::ShmMapError(e)
    }
}

impl From<ResourceError> for TransferError {
    fn from(e: ResourceError) -> TransferError {
        TransferError::ResourceError(e)
    }
}

impl From<Errno> for TransferError {
    fn from(e: Errno) -> TransferError {
        TransferError::ResourceError(ResourceError::Errno(e))
    }
}

impl From<RequestError> for TransferError {
    fn from(e: RequestError) -> TransferError {
        TransferError::RequestError(e)
    }
}

impl From<HeaderError> for RequestError {
    fn from(e: HeaderError) -> RequestError {
        RequestError::HeaderError(e)
    }
}
