use std::net::SocketAddr;

use anyhow::Error as AnyError;
use compio::{
    bytes::{BufMut, Bytes, BytesMut},
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    runtime::spawn,
    BufResult,
};
use redis_protocol::{
    error::{RedisParseError, RedisProtocolErrorKind},
    resp3::{
        decode::{
            self,
            complete::{decode_bytes, decode_bytes_mut},
        },
        encode::complete::encode_bytes,
        types::{BytesFrame, Resp3Frame},
    },
};

use crate::{
    command::{Command, CommandError},
    db::DBManager,
};

#[derive(Debug)]
pub struct Server {
    addr: SocketAddr,
}

impl Server {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    pub async fn run(mut self, db_manager: DBManager) -> Result<(), AnyError> {
        let listener = TcpListener::bind(self.addr).await?;

        loop {
            let (stream, addr) = listener.accept().await?;

            let service = Service::new(stream);
            spawn(service.handle(db_manager.clone())).detach();
        }

        Ok(())
    }
}

#[derive(Debug)]
struct Service {
    stream: TcpStream,
}

impl Service {
    fn new(stream: TcpStream) -> Self {
        Self { stream }
    }

    async fn handle(mut self, db_manager: DBManager) -> Result<(), AnyError> {
        let mut read_buf = BytesMut::with_capacity(128);
        let mut write_buf = BytesMut::with_capacity(128);

        'main: loop {
            let buf = [0u8; 64];
            let BufResult(read_size, buf) = self.stream.read(buf).await;

            match read_size? {
                0 => break 'main,
                n => {
                    read_buf.extend_from_slice(&buf[..n]);

                    match decode_bytes_mut(&mut read_buf) {
                        Ok(Some((frame, _, _))) => match Self::match_command(&frame) {
                            Ok(command) => {
                                db_manager.send_command(command);
                            }
                            Err(command_error) => {
                                let frame = BytesFrame::from(command_error);
                                let mut buf = Vec::with_capacity(frame.len());
                                encode_bytes(buf.as_mut_slice(), &frame, false).unwrap();
                                write_buf.extend_from_slice(&buf);

                                let buf = write_buf.split();
                                let buf = buf.freeze();
                                let BufResult(size, buf) = self.stream.write(buf).await;
                                size?;
                            }
                        },
                        Ok(None) => {}
                        Err(e) => match e.kind() {
                            RedisProtocolErrorKind::Parse => {}
                            _ => return Err(e.into()),
                        },
                    }
                }
            }
        }
        Ok(())
    }

    fn match_command(frame: &BytesFrame) -> Result<Command, CommandError> {
        let frame_data = if let BytesFrame::Array { data, attributes } = frame {
            data
        } else {
            return Err(CommandError::Unknown);
        };

        let Some((command, args)) = frame_data.split_first() else {
            return Err(CommandError::Unknown);
        };

        let command_name = if let BytesFrame::SimpleString { data, attributes } = command {
            data
        } else {
            return Err(CommandError::Unknown);
        };

        Command::new(command_name, args)
    }
}
