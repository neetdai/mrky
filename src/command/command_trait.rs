use compio::bytes::Bytes;
use crossbeam_channel::Sender;
use redis_protocol::resp3::types::BytesFrame;

use crate::db::Bucket;

use super::CommandError;

pub(crate) trait Cmd: Sized {
    fn parse(args: &[BytesFrame]) -> Result<Self, CommandError>;

    fn get_key(&self) -> &Bytes;

    fn apply(self, bucket: &mut Bucket) -> BytesFrame;
}
