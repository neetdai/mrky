use compio::bytes::Bytes;
use redis_protocol::{
    bytes_utils::Str,
    resp3::types::{BytesFrame, FrameKind, Resp3Frame},
};
use tracing::instrument;

use crate::{
    command::{command_trait::Cmd, CommandError},
    db::{Entry, Value},
};

#[derive(Debug)]
pub(crate) struct Set {
    pub(crate) key: Bytes,
    pub(crate) value: Bytes,
}

impl Cmd for Set {
    fn parse(args: &[redis_protocol::resp3::types::BytesFrame]) -> Result<Self, CommandError> {
        match args {
            [key, value] => {
                let key = if let BytesFrame::BlobString { data, attributes } = key {
                    data.clone()
                } else {
                    return Err(CommandError::Syntax);
                };

                let value = if let BytesFrame::BlobString { data, attributes } = value {
                    data.clone()
                } else {
                    return Err(CommandError::Invalid);
                };

                Ok(Self { key, value })
            }
            _ => Err(CommandError::Invalid),
        }
    }

    fn get_key(&self) -> &Bytes {
        &self.key
    }

    #[instrument]
    fn apply(self, bucket: &mut crate::db::Bucket) -> BytesFrame {
        match bucket.map.get(&self.key) {
            Some(entry) => {
                if let Entry::String(_) = entry {
                    bucket
                        .map
                        .insert(self.key, Entry::String(Value::new(self.value)));
                    BytesFrame::SimpleString {
                        data: Bytes::from_static(b"OK"),
                        attributes: None,
                    }
                } else {
                    BytesFrame::SimpleError {
                        data: Str::from_static(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        ),
                        attributes: None,
                    }
                }
            }
            None => {
                bucket
                    .map
                    .insert(self.key, Entry::String(Value::new(self.value)));
                BytesFrame::SimpleString {
                    data: Bytes::from_static(b"OK"),
                    attributes: None,
                }
            }
        }
    }
}
