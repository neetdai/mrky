use std::net::SocketAddr;

use compio::{bytes::{BufMut, Bytes, BytesMut}, io::AsyncRead, net::{TcpListener, TcpStream}, runtime::spawn, BufResult};
use anyhow::Error as AnyError;
use redis_protocol::{error::{RedisParseError, RedisProtocolErrorKind}, resp3::{decode::{self, complete::{decode_bytes, decode_bytes_mut}}, types::BytesFrame}};

use crate::command::Command;


#[derive(Debug)]
pub struct Server {
    addr: SocketAddr,
}

impl Server {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    pub async fn run(mut self) -> Result<(), AnyError> {
        let listener = TcpListener::bind(self.addr).await?;

        loop {
            let (stream, addr) = listener.accept().await?;

            let service = Service::new(stream);
            spawn(service.handle()).detach();
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

    async fn handle(mut self) -> Result<(), AnyError> {
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
                        Ok(Some((frame, _, _))) => {

                        },
                        Ok(None) => {

                        }
                        Err(e) => {
                            match e.kind() {
                                RedisProtocolErrorKind::Parse => {
                                }
                                _ => return Err(e.into()),
                            }
                        } 
                    }
                }
            }
        }
        Ok(())
    }

    fn match_command(frame: &BytesFrame) -> Result<Command, Bytes> {
        // match frame {
        //     BytesFrame::Array { data, attributes }  => {
        //         let frame_data = data;
        //         match frame_data.split_first() {
        //             Some((command, args)) => {
        //                 match command {
        //                     BytesFrame::SimpleString { data, attributes } => {
        //                         match data.as_ref() {
        //                             b"SET" => {

        //                             }
        //                             b"GET" => {

        //                             }
        //                             _ => {},
        //                         }
        //                     },
        //                     _ => {}
        //                 }
        //             }
        //             _ => {}
        //         }
        //     }
        //     _ => {

        //     }
        // }
        let frame_data = if let BytesFrame::Array { data, attributes } = frame {
            data
        } else {
            return Err(Bytes::from_static(b"unknown command"))
        };

        let Some((command, args)) = frame_data.split_first() else {
            return Err(Bytes::from_static(b"unknown command"))
        };

        let command_name = if let BytesFrame::SimpleString { data, attributes }  = command{
            data    
        } else {
            return Err(Bytes::from_static(b"unknown command"))
        };

        Ok(Command::new(command_name, args)?)
    }
}