use compio::bytes::Bytes;
use redis_protocol::resp3::types::BytesFrame;

use super::CommandError;

pub(crate) trait Cmd: Sized {
    fn parse(args: &[BytesFrame]) -> Result<Self, CommandError>;
}
