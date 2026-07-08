use nix::sys::socket::Backlog;

use std::time::Duration;

use rtipc::ChannelGroup;
use rtipc::Consumer;
use rtipc::PopResult;
use rtipc::Producer;

use rtipc::TryPushResult;

use rtipc::Server;

use crate::common::CommandId;
use crate::common::MsgCommand;
use crate::common::MsgEvent;
use crate::common::MsgResponse;

use crate::common::wait_pollin;

mod common;

struct App {
    command: Consumer<MsgCommand>,
    response: Producer<MsgResponse>,
    event: Producer<MsgEvent>,
}

fn print_group(grp: &ChannelGroup) {
    let attr = grp.get_attr();
    let grp_info = str::from_utf8(attr.info.iter().as_slice()).unwrap();
    let cmd_info = str::from_utf8(&attr.consumers.get(0).unwrap().info).unwrap();
    let rsp_info = str::from_utf8(&attr.producers.get(0).unwrap().info).unwrap();
    let evt_info = str::from_utf8(&attr.producers.get(1).unwrap().info).unwrap();
    println!(
        "server received request grp={} cmd={} rsp={} evt={}",
        grp_info, cmd_info, rsp_info, evt_info
    );
}

impl App {
    pub fn new(mut grp: ChannelGroup) -> Self {
        print_group(&grp);
        let command = grp.take_consumer(0).unwrap();
        let response = grp.take_producer(0).unwrap();
        let event = grp.take_producer(1).unwrap();

        Self {
            command,
            response,
            event,
        }
    }
    fn run(&mut self) {
        let mut run = true;
        let mut cnt = 0;

        while run {
            let eventfd = self.command.eventfd().unwrap();
            let _ = wait_pollin(eventfd, Duration::from_millis(10));
            match self.command.pop() {
                PopResult::QueueError => panic!(),
                PopResult::NoMessage => continue,
                PopResult::NoNewMessage => continue,
                PopResult::Success => {}
                PopResult::SuccessMessagesDiscarded => {}
            };
            let cmd = self.command.current_message().unwrap();
            self.response.current_message().id = cmd.id;
            let args: [i32; 3] = cmd.args;
            println!("server received command: {}", cmd);

            let cmdid: CommandId = unsafe { ::std::mem::transmute(cmd.id) };
            self.response.current_message().result = match cmdid {
                CommandId::Hello => 0,
                CommandId::Stop => {
                    run = false;
                    0
                }
                CommandId::SendEvent => {
                    self.send_events(args[0] as u32, args[1] as u32, args[2] != 0)
                }
                CommandId::Div => {
                    let (err, res) = self.div(args[0], args[1]);
                    self.response.current_message().data = res;
                    err
                }
            };
            self.response.force_push();

            cnt = cnt + 1;
        }
    }
    fn send_events(&mut self, id: u32, num: u32, force: bool) -> i32 {
        for i in 0..num {
            let event = self.event.current_message();
            event.id = id;
            event.nr = i;
            if force {
                self.event.force_push();
            } else {
                if self.event.try_push() == TryPushResult::QueueFull {
                    return i as i32;
                }
            }
        }
        num as i32
    }
    fn div(&mut self, a: i32, b: i32) -> (i32, i32) {
        if b == 0 {
            return (-1, 0);
        } else {
            return (0, a / b);
        }
    }
}

fn main() {
    let backlog = Backlog::new(1).unwrap();
    let server = Server::new("rtipc.sock", backlog).unwrap();
    let grp = server.conditional_accept(|_| true).unwrap();
    let mut app = App::new(grp);
    app.run();
}
