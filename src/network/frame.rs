use std::io::Read;
use std::io::Write;

use crate::CommandRequest;
use crate::CommandResponse;
use crate::KvError;
use bytes::Buf;
use bytes::BufMut;
use bytes::BytesMut;
use flate2::read::GzDecoder;
use flate2::{write::GzEncoder, Compression};
use prost::Message;
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::debug;

/// take the highest bit of HEAD_LEN as the flag of whether or not to compress
pub const HIGH_LEN: usize = 4; // 4 bytes
const MAX_FRAME: usize = 2 * 1024 * 1024 * 1024; // 2GB

/// The MTU of Ethernet is 1500 bytes, and the IP header is 20 bytes, the TCP header is 20 bytes, so the maximum length of the TCP data is 1460 bytes.
/// Generally, TCP packages will contain some Options(such as time stamp), so the maximum length of the TCP data is less than 1460 bytes. In that reserve 20 bytes for the Options, the maximum length of the TCP data is 1440 bytes.
/// then subtract 4 bytes of length,which means 1436, the maximum message length without sharding.
/// If the message is larger than 1436 bytes, it will be sharded into multiple frames.
const COMPRESSION_LIMIT: usize = 1436;

/// A bit flag used to mark the compressed data.
const COMPRESSION_BIT: usize = 1 << 31;
pub trait FrameCoder
where
    Self: Sized + Message + Default,
{
    // write to the passed `prost::Message` into a frame
    fn encode_frame(&self, buf: &mut BytesMut) -> Result<(), KvError> {
        let size = self.encoded_len();

        if size > MAX_FRAME {
            return Err(KvError::FrameError);
        }

        buf.put_u32(size as _);

        // if the size is larger than the limit, we need to compress it
        if size > COMPRESSION_LIMIT {
            let mut buf1 = Vec::with_capacity(size);
            self.encode(&mut buf1).unwrap();
            let payload = buf.split_off(HIGH_LEN);
            buf.clear();

            // need to compress
            let mut encoder = GzEncoder::new(payload.writer(), Compression::default());
            encoder.write_all(&buf1[..])?;

            // once the compression is done, we need to get the BytesMut back from the gzip encoder
            let payload = encoder.finish()?.into_inner();
            debug!("Encode a frame: size {}({})", size, payload.len());

            buf.put_u32((payload.len() | COMPRESSION_BIT) as _);

            // merge the payload back into the buffer
            buf.unsplit(payload);

            Ok(())
        } else {
            self.encode(buf).unwrap();
            Ok(())
        }
    }
    // decode a frame into the Message
    fn decode_frame(buf: &mut BytesMut) -> Result<Self, KvError> {
        let header = buf.get_u32() as usize;
        let (len, compressed) = decode_header(header);
        debug!("Got a frame: msg len {}, compressed: {}", len, compressed);

        if compressed {
            // decompress
            let mut decoder = GzDecoder::new(&buf[..len]);
            let mut buf1 = Vec::with_capacity(len * 2);
            decoder.read_to_end(&mut buf1)?;
            buf.advance(len);

            Ok(Self::decode(&buf1[..buf1.len()])?)
        } else {
            let msg = Self::decode(&buf[..len])?;
            buf.advance(len);
            Ok(msg)
        }
    }
}

impl FrameCoder for CommandRequest {}
impl FrameCoder for CommandResponse {}

pub fn decode_header(header: usize) -> (usize, bool) {
    let len = header & !COMPRESSION_BIT;
    let compressed = header & COMPRESSION_BIT == COMPRESSION_BIT;
    (len, compressed)
}

/// Read a complete frame from stream
pub async fn read_frame<S>(stream: &mut S, buf: &mut BytesMut) -> Result<(), KvError>
where
    S: AsyncRead + Unpin + Send,
{
    let header = stream.read_u32().await? as usize;
    let (len, _compressed) = decode_header(header);

    // if there is no such memory, allocate at least on frame to make it available
    buf.reserve(HIGH_LEN + len);
    buf.put_u32(header as _);

    unsafe { buf.advance_mut(len) };

    stream.read_exact(&mut buf[HIGH_LEN..]).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{utils::DummyStream, Value};
    use bytes::Bytes;

    use super::*;

    #[test]
    fn command_request_encode_decode_should_work() {
        let mut buf = BytesMut::new();

        let cmd = CommandRequest::hdel("t1", "hello");
        cmd.encode_frame(&mut buf).unwrap();

        assert!(!is_compressed(&buf));

        let res = CommandRequest::decode_frame(&mut buf).unwrap();
        assert_eq!(cmd, res);
    }

    #[test]
    fn command_response_encode_decode_should_work() {
        let mut buf = BytesMut::new();
        let values: Vec<Value> = vec![1.into(), "hello".into(), "world".into()];
        let res: CommandResponse = values.into();
        res.encode_frame(&mut buf).unwrap();

        assert!(!is_compressed(&buf));

        let result = CommandResponse::decode_frame(&mut buf).unwrap();
        assert_eq!(res, result);
    }

    #[test]
    fn command_response_compressed_encode_decode_should_work() {
        let mut buf = BytesMut::new();
        let value: Value = Bytes::from(vec![0u8; COMPRESSION_LIMIT + 1]).into();
        let res: CommandResponse = value.into();
        res.encode_frame(&mut buf).unwrap();

        assert!(is_compressed(&buf));

        let result = CommandResponse::decode_frame(&mut buf).unwrap();
        assert_eq!(res, result);
    }

    fn is_compressed(data: &[u8]) -> bool {
        let header = u32::from_be_bytes(data[..4].try_into().unwrap()) as usize;
        header & COMPRESSION_BIT == COMPRESSION_BIT
    }

    #[tokio::test]
    async fn read_frame_should_work() {
        let mut buf = BytesMut::new();
        let cmd = CommandRequest::hdel("t1", "hello");
        cmd.encode_frame(&mut buf).unwrap();

        let mut stream = DummyStream { buf };
        let mut data = BytesMut::new();

        read_frame(&mut stream, &mut data).await.unwrap();

        let res = CommandRequest::decode_frame(&mut data).unwrap();
        assert_eq!(cmd, res);
    }
}
