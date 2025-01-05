mod command_trait;
mod string;

use compio::bytes::Bytes;
use redis_protocol::resp3::types::BytesFrame;
use string::*;

#[derive(Debug)]
pub(crate) enum Command {
    Set(set::Set),
    Get(get::Get),
}

impl Command {
    pub(crate) fn new(command_name: &[u8], args: &[BytesFrame]) -> Result<Self, Bytes> {
        todo!()
    }
}