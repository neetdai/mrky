use compio::bytes::Bytes;
use redis_protocol::{bytes_utils::Str, resp3::types::BytesFrame};

use crate::{
    command::{command_trait::Cmd, CommandError},
    db::Entry,
};

#[derive(Debug)]
pub struct Get {
    pub key: Bytes,
}

impl Cmd for Get {
    fn parse(args: &[redis_protocol::resp3::types::BytesFrame]) -> Result<Self, CommandError> {
        match args {
            [key] => {
                let key = if let BytesFrame::BlobString { data, attributes } = key {
                    data.clone()
                } else {
                    return Err(CommandError::Syntax);
                };

                Ok(Self { key })
            }
            _ => Err(CommandError::Invalid),
        }
    }

    fn get_key(&self) -> &Bytes {
        &self.key
    }

    fn apply(self, bucket: &mut crate::db::Bucket) -> BytesFrame {
        match bucket.map.get(&self.key) {
            Some(entry) => match entry {
                Entry::String(value) => BytesFrame::SimpleString {
                    data: value.get().clone(),
                    attributes: None,
                },
                _ => BytesFrame::SimpleError {
                    data: Str::from_static(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                    attributes: None,
                },
            },
            None => BytesFrame::Null,
        }
    }
}
