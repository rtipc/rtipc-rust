use rtipc::{ChannelAttr, GroupAttr};
use std::fmt;
use std::num::NonZeroUsize;

#[allow(dead_code)]
pub const MSG_COMMAND_INFO: &[u8] = &[
    0x2, 0x2, 0x2, 0x69, 0x64, 0x1, 0x13, 0x4, 0x61, 0x72, 0x67, 0x73, 0x5, 0x2, 0x3,
];

#[allow(dead_code)]
pub const MSG_RESPONSE_INFO: &[u8] = &[
    0x2, 0x3, 0x2, 0x69, 0x64, 0x1, 0x13, 0x6, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x1, 0x3, 0x4,
    0x64, 0x61, 0x74, 0x61, 0x1, 0x3,
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
#[derive(Copy, Clone, Debug)]
pub struct MsgCommand {
    pub id: u32,
    pub args: [i32; 3],
}

impl fmt::Display for MsgCommand {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "id:  {}", self.id)?;
        for (i, v) in self.args.iter().enumerate() {
            writeln!(f, "\targs[{}]: {}", i, v)?;
        }
        Ok(())
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct MsgResponse {
    pub id: u32,
    pub result: i32,
    pub data: i32,
}

impl fmt::Display for MsgResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "id:  {}", self.id)?;
        writeln!(f, "result:  {}", self.result)?;
        writeln!(f, "data:  {}", self.data)?;
        Ok(())
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct MsgEvent {
    pub id: u32,
    pub nr: u32,
}

impl fmt::Display for MsgEvent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "id:  {}", self.id)?;
        writeln!(f, "nr:  {}", self.nr)?;
        Ok(())
    }
}

pub fn client_group_rpc_create() -> GroupAttr {
    let c2s_channels: &[ChannelAttr] = &[ChannelAttr {
        additional_messages: 0,
        message_size: unsafe { NonZeroUsize::new_unchecked(size_of::<MsgCommand>()) },
        eventfd: true,
        info: MSG_COMMAND_INFO.to_vec(),
    }];

    let s2c_channels: &[ChannelAttr] = &[
        ChannelAttr {
            additional_messages: 0,
            message_size: unsafe { NonZeroUsize::new_unchecked(size_of::<MsgResponse>()) },
            eventfd: true,
            info: MSG_RESPONSE_INFO.to_vec(),
        },
        ChannelAttr {
            additional_messages: 10,
            message_size: unsafe { NonZeroUsize::new_unchecked(size_of::<MsgEvent>()) },
            eventfd: true,
            info: MSG_EVENT_INFO.to_vec(),
        },
    ];

    GroupAttr {
        producers: c2s_channels.to_vec(),
        consumers: s2c_channels.to_vec(),
        info: RPC_INFO.to_vec(),
    }
}
pub fn server_group_rpc_create() -> GroupAttr {
    let c2s_channels: &[ChannelAttr] = &[ChannelAttr {
        additional_messages: 0,
        message_size: unsafe { NonZeroUsize::new_unchecked(size_of::<MsgCommand>()) },
        eventfd: true,
        info: MSG_COMMAND_INFO.to_vec(),
    }];

    let s2c_channels: &[ChannelAttr] = &[
        ChannelAttr {
            additional_messages: 0,
            message_size: unsafe { NonZeroUsize::new_unchecked(size_of::<MsgResponse>()) },
            eventfd: true,
            info: MSG_RESPONSE_INFO.to_vec(),
        },
        ChannelAttr {
            additional_messages: 10,
            message_size: unsafe { NonZeroUsize::new_unchecked(size_of::<MsgEvent>()) },
            eventfd: true,
            info: MSG_EVENT_INFO.to_vec(),
        },
    ];

    GroupAttr {
        producers: c2s_channels.to_vec(),
        consumers: s2c_channels.to_vec(),
        info: RPC_INFO.to_vec(),
    }
}
