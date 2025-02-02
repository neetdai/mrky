use std::{fmt::Debug, sync::Arc};

use compio::bytes::Bytes;
use crossbeam_channel::Sender;
use redis_protocol::resp3::types::BytesFrame;
use tracing::{error, instrument};

use crate::db::{Bucket, DBManager};

use super::CommandError;

pub(crate) trait Cmd: Sized + Debug {
    fn parse(args: &[BytesFrame]) -> Result<Self, CommandError>;

    fn get_key(&self) -> &Bytes;

    #[instrument]
    fn send_command(self, manager: DBManager, sender: Sender<BytesFrame>)
    where
        Self: Send + 'static,
    {
        let index = manager.get_bucket(&self.get_key());
        let manager_sender = manager.get_sender(index);
        if let Err(e) = manager_sender.send((
            Box::new(move |bucket: &mut Bucket| -> BytesFrame { self.apply(bucket) }),
            sender,
        )) {
            error!("db send command error: {}", e);
        };
    }

    fn apply(self, bucket: &mut Bucket) -> BytesFrame;
}
