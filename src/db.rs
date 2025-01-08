use compio::{bytes::Bytes, runtime::spawn_blocking};
use crossbeam_channel::{bounded, Sender};
use redis_protocol::resp3::types::BytesFrame;
use tracing::{error, instrument};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use crate::command::Command;

#[derive(Debug)]
pub(crate) enum Entry {
    String(Value),
}

#[derive(Debug)]
pub(crate) enum Value {
    Inner(Bytes),
}

impl Value {
    pub(crate) fn new(value: Bytes) -> Self {
        Self::Inner(value)
    }

    pub(crate) fn get(&self) -> &Bytes {
        match self {
            Self::Inner(value) => value,
        }
    }
}

#[derive(Debug)]
pub(crate) struct Bucket {
    pub(crate) map: HashMap<Bytes, Entry>,
}

impl Bucket {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }
}

thread_local! {
    static BUCKET: RefCell<Bucket> = RefCell::new(Bucket::new());
}

#[derive(Debug)]
struct DBManagerInner {
    sender: Vec<Sender<(Command, Sender<BytesFrame>)>>,
}

impl DBManagerInner {
    fn new() -> Self {
        let cpu_num = 4usize;
        let mut buckets = Vec::with_capacity(cpu_num);
        for _ in 0..cpu_num {
            let (sender, receiver) = bounded::<(Command, Sender<BytesFrame>)>(128);

            spawn_blocking(move || {
                'spawn_blocking: loop {
                    match receiver.recv() {
                        Ok((command, sender)) => {
                            BUCKET.with_borrow_mut(|bucket| {
                                let frame = command.apply(bucket);
                                if let Err(e) = sender.try_send(frame) {
                                    error!("bucket send error: {}", e);
                                }
                            });
                        }
                        Err(e) => {
                            println!("db error: {}", e);
                            break 'spawn_blocking;
                        }
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

    #[instrument]
    pub fn send_command(&self, command: Command, sender: Sender<BytesFrame>) {
        let position = crc32fast::hash(command.get_key()) as usize;
        let index = position % self.inner.sender.len();

        if let Err(e) = self.inner.sender[index].send((command, sender)) {
            error!("db send command error: {}", e);
        }
    }
}
