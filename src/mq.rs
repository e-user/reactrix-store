// This file is part of reactrix-store.
//
// Copyright 2019-2020 Alexander Dorn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use log::{debug, error, info};
use rmp_serde as rmp;
use serde::Deserialize;
use std::net::Ipv4Addr;
use std::sync::mpsc;
use std::{thread, u16};
use url::Url;
use zmq::{Context, Socket, SocketEvent};

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Message {
    pub topic: String,
    pub data: Vec<u8>,
}

pub enum PublishMessage {
    Sequence(i64),
    Forward(Message),
}

pub type Tx = mpsc::Sender<PublishMessage>;

fn publish(context: &Context, address: Ipv4Addr, port: u16) -> Result<Tx, failure::Error> {
    let (tx, rx) = mpsc::channel::<PublishMessage>();
    let url = Url::parse(&format!("tcp://{}:{}", &address, &port))?;

    let socket = context.socket(zmq::PUB)?;
    socket.monitor(
        "inproc://monitor",
        SocketEvent::ACCEPTED as i32 + SocketEvent::CLOSED as i32,
    )?;
    socket.bind(&url.clone().into_string())?;

    info!("ØMQ publish socket listening on {}", &url);

    thread::spawn(move || {
        for message in rx {
            match message {
                PublishMessage::Sequence(id) => {
                    debug!("Notify sequence {}", id);

                    let bytes = match rmp::to_vec(&id) {
                        Ok(bytes) => bytes,
                        Err(e) => {
                            error!("{}", e);
                            continue;
                        }
                    };

                    if let Err(e) = socket
                        .send("sequence", zmq::SNDMORE)
                        .and_then(|_| socket.send(&bytes, 0))
                    {
                        error!("{}", e);
                    }
                }

                PublishMessage::Forward(Message { topic, data }) => {
                    debug!("Forward message {}", topic);

                    if let Err(e) = socket
                        .send(&topic, zmq::SNDMORE)
                        .and_then(|_| socket.send(&data, 0))
                    {
                        error!("{}", e);
                    }
                }
            }
        }
    });

    Ok(tx)
}

fn poll_monitor(name: String, monitor: Socket) {
    thread::spawn(move || loop {
        if let Ok(message) = monitor.recv_msg(0) {
            let event = u16::from_ne_bytes([message[0], message[1]]);
            let _address = monitor.recv_string(0);

            match SocketEvent::from_raw(event) {
                SocketEvent::ACCEPTED => debug!("{} connection accepted", name),
                SocketEvent::CLOSED => debug!("{} connection closed", name),
                _ => error!("Unexpected event"),
            }
        }
    });
}

pub fn launch(address: Ipv4Addr, port: u16) -> Result<Tx, failure::Error> {
    let context = Context::new();
    let tx = publish(&context, address, port)?;

    let publish_monitor = context.socket(zmq::PAIR)?;
    publish_monitor.connect("inproc://monitor")?;

    poll_monitor("Publish".to_string(), publish_monitor);

    Ok(tx)
}
