use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::thread::JoinHandle;
use std::time;
use std::time::Duration;

use nix::errno::Errno;

use rtipc::ChannelGroup;
use rtipc::Consumer;
use rtipc::PopResult;
use rtipc::Producer;
use rtipc::client_connect;
use rtipc::error::*;
use rtipc::{ChannelAttr, GroupAttr};

use crate::common::CommandId;
use crate::common::MsgCommand;
use crate::common::MsgEvent;
use crate::common::MsgResponse;
use crate::common::wait_pollin;

mod common;

static STOP_EVENT_LISTERNER: AtomicBool = AtomicBool::new(false);

fn handle_events(mut consumer: Consumer<MsgEvent>) -> Result<(), Errno> {
    while !STOP_EVENT_LISTERNER.load(Ordering::Relaxed) {
        let eventfd = consumer.eventfd().unwrap();
        let ev = wait_pollin(eventfd, Duration::from_millis(10))?;

        if !ev {
            continue;
        }

        match consumer.pop() {
            PopResult::QueueError => panic!(),
            PopResult::NoMessage => return Err(Errno::EBADMSG),
            PopResult::NoNewMessage => return Err(Errno::EBADMSG),
            PopResult::Success => {
                println!(
                    "client received event: {}",
                    consumer.current_message().unwrap()
                )
            }
            PopResult::SuccessMessagesDiscarded => {
                println!(
                    "client received event: {}",
                    consumer.current_message().unwrap()
                )
            }
        };
    }
    println!("handle_events returns");
    Ok(())
}

struct App {
    command: Producer<MsgCommand>,
    response: Consumer<MsgResponse>,
    event_listener: Option<JoinHandle<Result<(), Errno>>>,
}

impl App {
    pub fn new(mut grp: ChannelGroup) -> Self {
        let command = grp.acquire_producer(0).unwrap();
        let response = grp.acquire_consumer(0).unwrap();
        let event = grp.acquire_consumer(1).unwrap();

        let event_listener = Some(thread::spawn(move || handle_events(event)));

        Self {
            command,
            response,
            event_listener,
        }
    }

    pub fn run(&mut self, cmds: &[MsgCommand]) {
        let pause = time::Duration::from_millis(10);

        for cmd in cmds {
            self.command.current_message().clone_from(cmd);
            self.command.force_push();

            loop {
                match self.response.pop() {
                    PopResult::QueueError => panic!(),
                    PopResult::NoMessage => {
                        thread::sleep(pause);
                        continue;
                    }
                    PopResult::NoNewMessage => {
                        thread::sleep(pause);
                        continue;
                    }
                    PopResult::Success => {}
                    PopResult::SuccessMessagesDiscarded => {}
                };

                println!(
                    "client received response: {}",
                    self.response.current_message().unwrap()
                );
                break;
            }
        }
        thread::sleep(time::Duration::from_millis(100));
        STOP_EVENT_LISTERNER.store(true, Ordering::Relaxed);
        self.event_listener.take().map(|h| h.join());
    }
}

fn main() {
    let commands: [MsgCommand; 6] = [
        MsgCommand {
            id: CommandId::Hello as u32,
            args: [1, 2, 0],
        },
        MsgCommand {
            id: CommandId::SendEvent as u32,
            args: [11, 20, 0],
        },
        MsgCommand {
            id: CommandId::SendEvent as u32,
            args: [12, 20, 1],
        },
        MsgCommand {
            id: CommandId::Div as u32,
            args: [100, 7, 0],
        },
        MsgCommand {
            id: CommandId::Div as u32,
            args: [100, 0, 0],
        },
        MsgCommand {
            id: CommandId::Stop as u32,
            args: [0, 0, 0],
        },
    ];

    let c2s_channels: [ChannelAttr; 1] = [ChannelAttr {
        additional_messages: 0,
        message_size: unsafe { NonZeroUsize::new_unchecked(size_of::<MsgCommand>()) },
        eventfd: true,
        info: b"rpc command".to_vec(),
    }];

    let s2c_channels: [ChannelAttr; 2] = [
        ChannelAttr {
            additional_messages: 0,
            message_size: unsafe { NonZeroUsize::new_unchecked(size_of::<MsgResponse>()) },
            eventfd: false,
            info: b"rpc response".to_vec(),
        },
        ChannelAttr {
            additional_messages: 10,
            message_size: unsafe { NonZeroUsize::new_unchecked(size_of::<MsgEvent>()) },
            eventfd: true,
            info: b"rpc event".to_vec(),
        },
    ];

    let attr = GroupAttr {
        producers: c2s_channels.to_vec(),
        consumers: s2c_channels.to_vec(),
        info: b"rpc example".to_vec(),
    };
    let grp = client_connect("rtipc.sock", &attr).unwrap();
    let mut app = App::new(grp);
    thread::sleep(time::Duration::from_millis(100));
    app.run(&commands);
}
