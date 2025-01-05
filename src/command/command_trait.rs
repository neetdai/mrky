use compio::bytes::Bytes;
use redis_protocol::resp3::types::BytesFrame;


pub trait Cmd: Sized {
    fn parse(args: &[BytesFrame]) -> Result<Self, Bytes>;

    
}