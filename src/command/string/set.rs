use compio::bytes::Bytes;
use redis_protocol::resp3::types::BytesFrame;

use crate::command::{command_trait::Cmd, CommandError};

#[derive(Debug)]
pub(crate) struct Set {
    pub(crate) key: Bytes,
    pub(crate) value: Bytes,
}

impl Cmd for Set {
    fn parse(args: &[redis_protocol::resp3::types::BytesFrame]) -> Result<Self, CommandError> {
        match args {
            [key, value] => {
                let key = if let BytesFrame::SimpleString { data, attributes } = key {
                    data.clone()
                } else {
                    return Err(CommandError::Syntax);
                };

                let value = if let BytesFrame::SimpleString { data, attributes } = value {
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
}
