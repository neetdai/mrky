use compio::bytes::Bytes;
use crossbeam_channel::{bounded, Sender};
use redis_protocol::resp3::types::BytesFrame;
use tracing::{error, instrument};

use crate::{
    command::{command_trait::Cmd, list, CommandError},
    db::{Bucket, DBManager},
};

use super::get::Get;

#[derive(Debug)]
pub struct Mget {
    pub keys: Vec<Bytes>,
}

impl Cmd for Mget {
    fn parse(args: &[redis_protocol::resp3::types::BytesFrame]) -> Result<Self, CommandError> {
        let keys = args
            .iter()
            .map(|frame| {
                if let BytesFrame::BlobString { data, attributes } = frame {
                    Ok(data.clone())
                } else {
                    error!("Invalid argument type for MGET command");
                    Err(CommandError::Invalid)
                }
            })
            .collect::<Result<Vec<Bytes>, CommandError>>()?;
        Ok(Self { keys })
    }

    fn get_key(&self) -> &Bytes {
        unreachable!()
    }

    #[instrument]
    fn send_command(self, manager: DBManager, sender: Sender<BytesFrame>)
    where
        Self: Send + 'static,
    {
        let key_len = self.keys.len();
        let (get_sender, get_receiver) = bounded(key_len);
        let mut list = Vec::with_capacity(key_len);

        for get in self.keys.into_iter().map(|key| {
            let get = Get { key };
            get
        }) {
            get.send_command(manager.clone(), get_sender.clone());
            list.push(get_receiver.recv().unwrap());
        }

        if let Err(e) = sender.try_send(BytesFrame::Array { data: list, attributes: None }) {
            error!("Failed to send response: {:?}", e);
        }
    }

    fn apply(self, bucket: &mut crate::db::Bucket) -> redis_protocol::resp3::types::BytesFrame {
        todo!()
    }
}
