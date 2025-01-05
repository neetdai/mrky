use compio::bytes::Bytes;


#[derive(Debug)]
pub struct Get {
    pub key: Bytes,
}