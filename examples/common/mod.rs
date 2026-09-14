use std::fmt;

use std::os::fd::BorrowedFd;
use std::time::Duration;

use nix::errno::Errno;
use nix::poll::{PollFd, PollFlags, PollTimeout, poll};

pub use crate::common::rpc::MsgCommand;
pub use crate::common::rpc::MsgEvent;
pub use crate::common::rpc::MsgResponse;

pub mod rpc;

#[repr(u32)]
#[derive(Copy, Clone, Debug)]
pub enum CommandId {
    Hello = 1,
    Stop = 2,
    SendEvent = 3,
    Div = 4,
}

pub fn wait_pollin(fd: BorrowedFd, timeout: Duration) -> Result<bool, Errno> {
    let mut fds = [PollFd::new(fd, PollFlags::POLLIN)];
    let duration: PollTimeout = timeout.try_into().unwrap();
    poll(&mut fds, duration)?;
    Ok(fds[0].revents().map_or(false, |flags| !flags.is_empty()))
}
