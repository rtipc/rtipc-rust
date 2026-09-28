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

use crate::common::CommandArgs;
use crate::common::CommandId;
use crate::common::DivArgs;
use crate::common::MsgCommand;
use crate::common::MsgEvent;
use crate::common::MsgResponse;
use crate::common::SendEventArgs;
use crate::common::rpc::CLIENT_GROUP_RPC_CREATE;
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

        match consumer.pop().unwrap() {
            PopResult::NoMessage => return Err(Errno::EBADMSG),
            PopResult::NoNewMessage => return Err(Errno::EBADMSG),
            PopResult::Success => {
                let msg = consumer.current_message().unwrap();
                println!("client received event: id = {} nr = {}", msg.id, msg.nr)
            }
            PopResult::SuccessMessagesDiscarded => {
                let msg = consumer.current_message().unwrap();
                println!("client received event: id = {} nr = {}", msg.id, msg.nr)
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
            self.command.force_push().unwrap();

            loop {
                match self.response.pop().unwrap() {
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
                let msg = self.response.current_message().unwrap();
                println!(
                    "client received response id = {}, result = {} ",
                    msg.id, msg.result
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
            args: unsafe { std::mem::zeroed() },
        },
        MsgCommand {
            id: CommandId::SendEvent as u32,
            args: CommandArgs {
                send: SendEventArgs {
                    id: 11,
                    force: false,
                    num: 20,
                },
            },
        },
        MsgCommand {
            id: CommandId::SendEvent as u32,
            args: CommandArgs {
                send: SendEventArgs {
                    id: 12,
                    force: true,
                    num: 20,
                },
            },
        },
        MsgCommand {
            id: CommandId::Div as u32,
            args: CommandArgs {
                div: DivArgs {
                    divisor: 100.0,
                    divident: 7.0,
                },
            },
        },
        MsgCommand {
            id: CommandId::Div as u32,
            args: CommandArgs {
                div: DivArgs {
                    divisor: 100.0,
                    divident: 0.0,
                },
            },
        },
        MsgCommand {
            id: CommandId::Stop as u32,
            args: unsafe { std::mem::zeroed() },
        },
    ];

    let attr = &CLIENT_GROUP_RPC_CREATE;
    let grp = client_connect("rtipc.sock", attr).unwrap();
    let mut app = App::new(grp);
    thread::sleep(time::Duration::from_millis(100));
    app.run(&commands);
}
