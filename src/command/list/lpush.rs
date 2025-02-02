use crate::command::command_trait::Cmd;
use crate::command::CommandError;
use crate::db::{Entry, Value};
use compio::bytes::Bytes;
use redis_protocol::bytes_utils::Str;
use redis_protocol::resp3::types::BytesFrame;

#[derive(Debug)]
pub struct Lpush {
    pub key: Bytes,
    pub values: Vec<Bytes>,
}

impl Cmd for Lpush {
    fn parse(
        args: &[redis_protocol::resp3::types::BytesFrame],
    ) -> Result<Self, crate::command::CommandError> {
        if args.len() < 2 {
            return Err(crate::command::CommandError::Invalid);
        }

        let (key, values) = match args.split_at(1) {
            (
                &[BytesFrame::BlobString {
                    ref data,
                    ref attributes,
                }],
                values,
            ) => {
                let values = values
                    .iter()
                    .map(|frame| {
                        if let BytesFrame::BlobString { data, attributes } = frame {
                            Ok(data.clone())
                        } else {
                            Err(crate::command::CommandError::Invalid)
                        }
                    })
                    .collect::<Result<Vec<Bytes>, crate::command::CommandError>>()?;
                (data.clone(), values)
            }
            _ => return Err(crate::command::CommandError::Invalid),
        };

        Ok(Lpush { key, values })
    }

    fn get_key(&self) -> &Bytes {
        &self.key
    }

    fn apply(self, bucket: &mut crate::db::Bucket) -> BytesFrame {
        match bucket.map.get_mut(&self.key) {
            Some(entry) => {
                if let Entry::List(list) = entry {
                    list.extend(self.values.into_iter().map(|v| Value::new(v)));
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
                bucket.map.insert(
                    self.key,
                    Entry::List(self.values.into_iter().map(|v| Value::new(v)).collect()),
                );
                BytesFrame::SimpleString {
                    data: Bytes::from_static(b"OK"),
                    attributes: None,
                }
            }
        }
    }
}
