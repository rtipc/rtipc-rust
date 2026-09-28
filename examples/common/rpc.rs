use rtipc::error::*;
use rtipc::{ChannelAttributes, ChannelGroup, Consumer, GroupAttributes, Producer};
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::LazyLock;

#[allow(dead_code)]
pub const SEND_EVENT_ARGS_INFO: &[u8] = &[
    0x2, 0x3, 0x2, 0x69, 0x64, 0x1, 0x13, 0x5, 0x66, 0x6f, 0x72, 0x63, 0x65, 0x1, 0x0, 0x3, 0x6e,
    0x75, 0x6d, 0x1, 0x13,
];

#[allow(dead_code)]
pub const DIV_ARGS_INFO: &[u8] = &[
    0x2, 0x2, 0x7, 0x64, 0x69, 0x76, 0x69, 0x73, 0x6f, 0x72, 0x1, 0x24, 0x8, 0x64, 0x69, 0x76,
    0x69, 0x64, 0x65, 0x6e, 0x74, 0x1, 0x24,
];

#[allow(dead_code)]
pub const COMMAND_ARGS_INFO: &[u8] = &[
    0x3, 0x2, 0x3, 0x64, 0x69, 0x76, 0x2, 0x2, 0x7, 0x64, 0x69, 0x76, 0x69, 0x73, 0x6f, 0x72, 0x1,
    0x24, 0x8, 0x64, 0x69, 0x76, 0x69, 0x64, 0x65, 0x6e, 0x74, 0x1, 0x24, 0x4, 0x73, 0x65, 0x6e,
    0x64, 0x2, 0x3, 0x2, 0x69, 0x64, 0x1, 0x13, 0x5, 0x66, 0x6f, 0x72, 0x63, 0x65, 0x1, 0x0, 0x3,
    0x6e, 0x75, 0x6d, 0x1, 0x13,
];

#[allow(dead_code)]
pub const MSG_COMMAND_INFO: &[u8] = &[
    0x2, 0x2, 0x2, 0x69, 0x64, 0x1, 0x13, 0x4, 0x61, 0x72, 0x67, 0x73, 0x3, 0x2, 0x3, 0x64, 0x69,
    0x76, 0x2, 0x2, 0x7, 0x64, 0x69, 0x76, 0x69, 0x73, 0x6f, 0x72, 0x1, 0x24, 0x8, 0x64, 0x69,
    0x76, 0x69, 0x64, 0x65, 0x6e, 0x74, 0x1, 0x24, 0x4, 0x73, 0x65, 0x6e, 0x64, 0x2, 0x3, 0x2,
    0x69, 0x64, 0x1, 0x13, 0x5, 0x66, 0x6f, 0x72, 0x63, 0x65, 0x1, 0x0, 0x3, 0x6e, 0x75, 0x6d, 0x1,
    0x13,
];

#[allow(dead_code)]
pub const RESPONSE_DATA_INFO: &[u8] = &[
    0x3, 0x1, 0x8, 0x71, 0x75, 0x6f, 0x74, 0x69, 0x65, 0x6e, 0x74, 0x1, 0x24,
];

#[allow(dead_code)]
pub const MSG_RESPONSE_INFO: &[u8] = &[
    0x2, 0x3, 0x2, 0x69, 0x64, 0x1, 0x13, 0x6, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x1, 0x3, 0x4,
    0x64, 0x61, 0x74, 0x61, 0x3, 0x1, 0x8, 0x71, 0x75, 0x6f, 0x74, 0x69, 0x65, 0x6e, 0x74, 0x1,
    0x24,
];

#[allow(dead_code)]
pub const MSG_EVENT_INFO: &[u8] = &[
    0x2, 0x2, 0x2, 0x69, 0x64, 0x1, 0x13, 0x2, 0x6e, 0x72, 0x1, 0x13,
];

#[allow(dead_code)]
pub const RPC_INFO: &[u8] = &[
    0x3, 0x52, 0x50, 0x43, 0x7, 0x63, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x8, 0x72, 0x65, 0x73,
    0x70, 0x6f, 0x6e, 0x73, 0x65, 0x5, 0x65, 0x76, 0x65, 0x6e, 0x74,
];

#[repr(C)]
#[derive(Copy, Clone)]
pub struct SendEventArgs {
    pub id: u32,
    pub force: bool,
    pub num: u32,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct DivArgs {
    pub divisor: f64,
    pub divident: f64,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub union CommandArgs {
    pub div: DivArgs,
    pub send: SendEventArgs,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct MsgCommand {
    pub id: u32,
    pub args: CommandArgs,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub union ResponseData {
    pub quotient: f64,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct MsgResponse {
    pub id: u32,
    pub result: i32,
    pub data: ResponseData,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct MsgEvent {
    pub id: u32,
    pub nr: u32,
}

#[allow(dead_code)]
pub static CLIENT_GROUP_RPC_CREATE: LazyLock<GroupAttributes> = LazyLock::new(|| {
    let c2s_channels: &[ChannelAttributes] = &[ChannelAttributes {
        additional_messages: 0,
        message_size: unsafe { NonZeroUsize::new_unchecked(size_of::<MsgCommand>()) },
        eventfd: true,
        info: MSG_COMMAND_INFO.to_vec(),
    }];

    let s2c_channels: &[ChannelAttributes] = &[
        ChannelAttributes {
            additional_messages: 0,
            message_size: unsafe { NonZeroUsize::new_unchecked(size_of::<MsgResponse>()) },
            eventfd: true,
            info: MSG_RESPONSE_INFO.to_vec(),
        },
        ChannelAttributes {
            additional_messages: 10,
            message_size: unsafe { NonZeroUsize::new_unchecked(size_of::<MsgEvent>()) },
            eventfd: true,
            info: MSG_EVENT_INFO.to_vec(),
        },
    ];

    GroupAttributes {
        producers: c2s_channels.to_vec(),
        consumers: s2c_channels.to_vec(),
        info: RPC_INFO.to_vec(),
    }
});
#[allow(dead_code)]
pub fn client_rpc_acquire_response(
    group: &mut ChannelGroup,
) -> Result<Consumer<MsgResponse>, AcquireError> {
    let group_attr = group.get_attr();
    let remote_attr = group_attr
        .consumers
        .get(0)
        .ok_or(AcquireError::OutOfBounds)?;

    let expect_attr = CLIENT_GROUP_RPC_CREATE
        .consumers
        .get(0)
        .ok_or(AcquireError::OutOfBounds)?;

    if expect_attr != remote_attr {
        return Err(AcquireError::AttrMismatch);
    }

    group.acquire_consumer(1).ok_or(AcquireError::OutOfBounds)
}
#[allow(dead_code)]
pub fn client_rpc_acquire_event(
    group: &mut ChannelGroup,
) -> Result<Consumer<MsgEvent>, AcquireError> {
    let group_attr = group.get_attr();
    let remote_attr = group_attr
        .consumers
        .get(1)
        .ok_or(AcquireError::OutOfBounds)?;

    let expect_attr = CLIENT_GROUP_RPC_CREATE
        .consumers
        .get(1)
        .ok_or(AcquireError::OutOfBounds)?;

    if expect_attr != remote_attr {
        return Err(AcquireError::AttrMismatch);
    }

    group.acquire_consumer(1).ok_or(AcquireError::OutOfBounds)
}
#[allow(dead_code)]
pub fn client_rpc_acquire_command(
    group: &mut ChannelGroup,
) -> Result<Producer<MsgCommand>, AcquireError> {
    let group_attr = group.get_attr();
    let remote_attr = group_attr
        .producers
        .get(0)
        .ok_or(AcquireError::OutOfBounds)?;

    let expect_attr = CLIENT_GROUP_RPC_CREATE
        .producers
        .get(0)
        .ok_or(AcquireError::OutOfBounds)?;

    if expect_attr != remote_attr {
        return Err(AcquireError::AttrMismatch);
    }

    group.acquire_producer(1).ok_or(AcquireError::OutOfBounds)
}
#[allow(dead_code)]
pub static SERVER_GROUP_RPC_CREATE: LazyLock<GroupAttributes> = LazyLock::new(|| {
    let c2s_channels: &[ChannelAttributes] = &[ChannelAttributes {
        additional_messages: 0,
        message_size: unsafe { NonZeroUsize::new_unchecked(size_of::<MsgCommand>()) },
        eventfd: true,
        info: MSG_COMMAND_INFO.to_vec(),
    }];

    let s2c_channels: &[ChannelAttributes] = &[
        ChannelAttributes {
            additional_messages: 0,
            message_size: unsafe { NonZeroUsize::new_unchecked(size_of::<MsgResponse>()) },
            eventfd: true,
            info: MSG_RESPONSE_INFO.to_vec(),
        },
        ChannelAttributes {
            additional_messages: 10,
            message_size: unsafe { NonZeroUsize::new_unchecked(size_of::<MsgEvent>()) },
            eventfd: true,
            info: MSG_EVENT_INFO.to_vec(),
        },
    ];

    GroupAttributes {
        producers: c2s_channels.to_vec(),
        consumers: s2c_channels.to_vec(),
        info: RPC_INFO.to_vec(),
    }
});
#[allow(dead_code)]
pub fn server_rpc_acquire_command(
    group: &mut ChannelGroup,
) -> Result<Consumer<MsgCommand>, AcquireError> {
    let group_attr = group.get_attr();
    let remote_attr = group_attr
        .consumers
        .get(0)
        .ok_or(AcquireError::OutOfBounds)?;

    let expect_attr = SERVER_GROUP_RPC_CREATE
        .consumers
        .get(0)
        .ok_or(AcquireError::OutOfBounds)?;

    if expect_attr != remote_attr {
        return Err(AcquireError::AttrMismatch);
    }

    group.acquire_consumer(1).ok_or(AcquireError::OutOfBounds)
}
#[allow(dead_code)]
pub fn server_rpc_acquire_response(
    group: &mut ChannelGroup,
) -> Result<Producer<MsgResponse>, AcquireError> {
    let group_attr = group.get_attr();
    let remote_attr = group_attr
        .producers
        .get(0)
        .ok_or(AcquireError::OutOfBounds)?;

    let expect_attr = SERVER_GROUP_RPC_CREATE
        .producers
        .get(0)
        .ok_or(AcquireError::OutOfBounds)?;

    if expect_attr != remote_attr {
        return Err(AcquireError::AttrMismatch);
    }

    group.acquire_producer(1).ok_or(AcquireError::OutOfBounds)
}
#[allow(dead_code)]
pub fn server_rpc_acquire_event(
    group: &mut ChannelGroup,
) -> Result<Producer<MsgEvent>, AcquireError> {
    let group_attr = group.get_attr();
    let remote_attr = group_attr
        .producers
        .get(1)
        .ok_or(AcquireError::OutOfBounds)?;

    let expect_attr = SERVER_GROUP_RPC_CREATE
        .producers
        .get(1)
        .ok_or(AcquireError::OutOfBounds)?;

    if expect_attr != remote_attr {
        return Err(AcquireError::AttrMismatch);
    }

    group.acquire_producer(1).ok_or(AcquireError::OutOfBounds)
}
