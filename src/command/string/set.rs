use compio::bytes::Bytes;
use redis_protocol::resp3::types::BytesFrame;

use crate::command::command_trait::Cmd;



#[derive(Debug)]
pub(crate) struct Set {
    pub(crate) key: Bytes,
    pub(crate) value: Bytes,
    pub(crate) expire: Option<i64>,
}

impl Cmd for Set {
    fn parse(args: &[redis_protocol::resp3::types::BytesFrame]) -> Result<Self, Bytes> {
        match args {
            [key,value] => {
                let key = if let BytesFrame::SimpleString { data, attributes } = key {
                    data.clone()
                } else {
                    return Err(Bytes::from_static(b"invalid key"));
                };

                let value = if let BytesFrame::SimpleString { data, attributes } = value {
                    data.clone()
                } else {
                    return Err(Bytes::from_static(b"invalid value"));
                };

                Ok(Self {
                    key,
                    value,
                    expire: None,
                })
            }
            _ => {
                return Err(Bytes::from_static(b"syntax error"));
            }
        }
    }
}