use compio::{bytes::Bytes, runtime::spawn_blocking};
use crossbeam_channel::{bounded, Sender};
use std::collections::HashMap;
use std::sync::Arc;

use crate::command::Command;

#[derive(Debug)]
enum Entry {
    String(Value),
}

#[derive(Debug)]
enum Value {
    Inner(Bytes),
}

#[derive(Debug)]
struct Bucket {
    map: HashMap<Bytes, Entry>,
}

#[derive(Debug)]
struct DBManagerInner {
    sender: Vec<Sender<Command>>,
}

impl DBManagerInner {
    fn new() -> Self {
        let cpu_num = 4usize;
        let mut buckets = Vec::with_capacity(cpu_num);
        for _ in 0..cpu_num {
            let (sender, receiver) = bounded(128);

            spawn_blocking(move || loop {
                match receiver.recv() {
                    Ok(command) => {}
                    Err(e) => {
                        println!("db error: {}", e);
                        break;
                    }
                }
            })
            .detach();

            buckets.push(sender);
        }

        Self { sender: buckets }
    }
}

#[derive(Debug, Clone)]
pub struct DBManager {
    inner: Arc<DBManagerInner>,
}

impl DBManager {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DBManagerInner::new()),
        }
    }

    pub fn send_command(&self, command: Command) {
        let position = crc32fast::hash(command.get_key()) as usize;
        let index = position % self.inner.sender.len();

        self.inner.sender[index].send(command).unwrap();
    }
}
