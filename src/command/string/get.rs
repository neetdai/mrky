use compio::bytes::Bytes;
use redis_protocol::resp3::types::BytesFrame;

use crate::command::{command_trait::Cmd, CommandError};

#[derive(Debug)]
pub struct Get {
    pub key: Bytes,
}

impl Cmd for Get {
    fn parse(args: &[redis_protocol::resp3::types::BytesFrame]) -> Result<Self, CommandError> {
        match args {
            [key] => {
                let key = if let BytesFrame::SimpleString { data, attributes } = key {
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
}
