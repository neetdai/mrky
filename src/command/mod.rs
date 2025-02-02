mod command_trait;
mod list;
mod string;

use command_trait::Cmd;
use compio::{
    buf::IoBufMut,
    bytes::{BufMut, Bytes, BytesMut},
};
use crossbeam_channel::Sender;
use list::*;
use redis_protocol::{
    bytes_utils::Str,
    resp3::{
        encode::complete::encode_bytes,
        types::{BytesFrame, Resp3Frame},
    },
};
use string::*;

use crate::db::{Bucket, DBManager};

#[derive(Debug)]
pub(crate) enum CommandError {
    Syntax,
    Invalid,
    Unknown,
}

impl From<CommandError> for BytesFrame {
    fn from(error: CommandError) -> Self {
        match error {
            CommandError::Syntax => BytesFrame::SimpleError {
                data: Str::from_static("syntax error"),
                attributes: None,
            },
            CommandError::Invalid => BytesFrame::SimpleError {
                data: Str::from_static("invalid value"),
                attributes: None,
            },
            CommandError::Unknown => BytesFrame::SimpleError {
                data: Str::from_static("unknown command"),
                attributes: None,
            },
        }
    }
}

#[derive(Debug)]
pub(crate) enum Command {
    Set(set::Set),
    Get(get::Get),
    Mget(mget::Mget),
}

impl Command {
    pub(crate) fn new(command_name: &[u8], args: &[BytesFrame]) -> Result<Self, CommandError> {
        match command_name {
            b"SET" => Ok(Command::Set(set::Set::parse(args)?)),
            b"GET" => Ok(Command::Get(get::Get::parse(args)?)),
            b"MGET" => Ok(Command::Mget(mget::Mget::parse(args)?)),
            _ => Err(CommandError::Unknown),
        }
    }

    pub(crate) fn send_command(self, manager: DBManager, sender: Sender<BytesFrame>) {
        match self {
            Self::Set(set) => set.send_command(manager, sender),
            Self::Get(get) => get.send_command(manager, sender),
            Self::Mget(mget) => mget.send_command(manager, sender),
        }
    }
}
