use compio::bytes::Bytes;
use redis_protocol::{bytes_utils::Str, resp3::types::BytesFrame};

use crate::{command::command_trait::Cmd, db::Entry};


#[derive(Debug)]
pub struct Lrange {
    pub key: Bytes,
}

impl Cmd for Lrange {
    fn parse(args: &[redis_protocol::resp3::types::BytesFrame]) -> Result<Self, crate::command::CommandError> {
        let key = match args {
            &[BytesFrame::BlobString {ref data, ref attributes}] => {
                data.clone()
            }
            _ => return Err(crate::command::CommandError::Invalid),
        };

        Ok(Self { key })
    }

    fn get_key(&self) -> &Bytes {
        &self.key
    }

    fn apply(self, bucket: &mut crate::db::Bucket) -> redis_protocol::resp3::types::BytesFrame {
        match bucket.map.get(&self.key) {
            Some(entry) => {
                if let Entry::List(list) = entry {
                    let new_list = list.iter()
                        .map(|value| {
                            let data = value.get().clone();
                            BytesFrame::BlobString { data, attributes: None }
                        })
                        .collect();
                    BytesFrame::Array{ data: new_list, attributes: None }
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
                BytesFrame::Null
            }
        }
    }
}