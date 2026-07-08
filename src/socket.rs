use nix::NixPath;
use nix::errno::Errno;
use nix::sys::socket::{
    AddressFamily, Backlog, SockFlag, SockType, UnixAddr, accept, bind, connect, listen, socket,
};
use nix::unistd::unlink;
use std::os::fd::{OwnedFd, RawFd};
use std::os::unix::io::AsRawFd;

use crate::GroupAttr;
use crate::channel::ChannelGroup;
use crate::error::*;
use crate::protocol::{create_response, parse_response};
use crate::unix::{UnixMessageRx, UnixMessageTx};

pub struct Server {
    sockfd: OwnedFd,
    addr: UnixAddr,
}

impl Server {
    pub fn new<P: ?Sized + NixPath>(path: &P, backlog: Backlog) -> Result<Self, Errno> {
        let addr = UnixAddr::new(path)?;
        let sockfd = socket(
            AddressFamily::Unix,
            SockType::SeqPacket,
            SockFlag::empty(),
            None,
        )?;
        bind(sockfd.as_raw_fd(), &addr)?;
        listen(&sockfd, backlog)?;
        Ok(Self { sockfd, addr })
    }

    fn handle_request<F>(socket: RawFd, filter: F) -> Result<ChannelGroup, TransferError>
    where
        F: Fn(&ChannelGroup) -> bool,
    {
        let mut req = UnixMessageRx::receive(socket.as_raw_fd())?;

        let fds = req.take_fds();

        let grp = ChannelGroup::deserialize(req.content(), fds)?;

        if !filter(&grp) {
            return Err(TransferError::Rejected);
        }

        Ok(grp)
    }

    pub fn conditional_accept<F>(&self, filter: F) -> Result<ChannelGroup, TransferError>
    where
        F: Fn(&ChannelGroup) -> bool,
    {
        let socket = accept(self.sockfd.as_raw_fd())?;

        let result = Self::handle_request(socket, filter);

        let response_msg = create_response(result.is_ok());

        let response = UnixMessageTx::new(response_msg, Vec::with_capacity(0));

        response.send(socket)?;
        result
    }

    pub fn accept(&self) -> Result<ChannelGroup, TransferError> {
        self.conditional_accept(|_| true)
    }
}

pub fn client_connect_fd(socket: RawFd, attr: &GroupAttr) -> Result<ChannelGroup, TransferError> {
    let grp = ChannelGroup::from_attr(attr)?;

    let (req_msg, fds) = grp.serialize();

    let req = UnixMessageTx::new(req_msg, fds);

    req.send(socket)?;

    let response = UnixMessageRx::receive(socket.as_raw_fd())?;

    parse_response(response.content().as_slice())?;

    Ok(grp)
}

pub fn client_connect<P: ?Sized + NixPath>(
    path: &P,
    attr: &GroupAttr,
) -> Result<ChannelGroup, TransferError> {
    let socket = socket(
        AddressFamily::Unix,
        SockType::SeqPacket,
        SockFlag::empty(),
        None,
    )?;

    let addr = UnixAddr::new(path)?;

    connect(socket.as_raw_fd(), &addr)?;

    let grp = ChannelGroup::from_attr(attr)?;

    let (req_msg, fds) = grp.serialize();

    let req = UnixMessageTx::new(req_msg, fds);

    req.send(socket.as_raw_fd())?;

    let response = UnixMessageRx::receive(socket.as_raw_fd())?;

    parse_response(response.content().as_slice())?;

    Ok(grp)
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(path) = self.addr.path() {
            let _ = unlink(path);
        }
    }
}
