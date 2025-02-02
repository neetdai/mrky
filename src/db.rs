use compio::{bytes::Bytes, runtime::spawn_blocking};
use crossbeam_channel::{bounded, Sender};
use hashbrown::HashMap;
use redis_protocol::resp3::types::BytesFrame;
use std::{cell::RefCell, collections::VecDeque};
use std::sync::Arc;
use tracing::{error, instrument};

use crate::command::Command;

#[derive(Debug)]
pub(crate) enum Entry {
    String(Value),
    List(VecDeque<Value>),
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
    sender: Vec<
        Sender<(
            Box<dyn FnOnce(&mut Bucket) -> BytesFrame + Send>,
            Sender<BytesFrame>,
        )>,
    >,
}

impl DBManagerInner {
    fn new() -> Self {
        let cpu_num = 4usize;
        let mut buckets = Vec::with_capacity(cpu_num);
        for _ in 0..cpu_num {
            let (sender, receiver) = bounded::<(
                Box<dyn FnOnce(&mut Bucket) -> BytesFrame + Send>,
                Sender<BytesFrame>,
            )>(128);

            spawn_blocking(move || 'spawn_blocking: loop {
                match receiver.recv() {
                    Ok((command_fn, sender)) => {
                        BUCKET.with_borrow_mut(|bucket| {
                            let frame = command_fn(bucket);
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

    // #[instrument]
    // pub fn send_command(&self, command: Command, sender: Sender<BytesFrame>) {
    //     let position = crc32fast::hash(command.get_key()) as usize;
    //     let index = position % self.inner.sender.len();

    //     if let Err(e) = self.inner.sender[index].send((command, sender)) {
    //         error!("db send command error: {}", e);
    //     }
    // }

    pub(crate) fn get_bucket(&self, key: &[u8]) -> usize {
        let position = crc32fast::hash(key) as usize;
        position % self.inner.sender.len()
    }

    pub(crate) fn get_sender(
        &self,
        index: usize,
    ) -> &Sender<(
        Box<dyn FnOnce(&mut Bucket) -> BytesFrame + Send>,
        Sender<BytesFrame>,
    )> {
        &self.inner.sender[index]
    }
}
