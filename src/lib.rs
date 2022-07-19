#[derive(Debug)]
enum Error {
    DecodeError(String),
}

type Result<T> = std::result::Result<T, Error>;

trait Encodable: Sized {
    fn encode(&self) -> Vec<u8>;
    fn decode(buf: &[u8]) -> Result<Self>;
}

#[derive(Debug, PartialEq, Eq)]
struct Header {
    timestamp: u32,
    key_size: u32,
    value_size: u32,
}

const HEADER_SIZE: usize = 12; // 12 bytes to encode three u32

impl Encodable for Header {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HEADER_SIZE);
        buf.extend_from_slice(&self.timestamp.to_le_bytes());
        buf.extend_from_slice(&self.key_size.to_le_bytes());
        buf.extend_from_slice(&self.value_size.to_le_bytes());
        buf
    }

    fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() != HEADER_SIZE {
            return Err(Error::DecodeError(format!(
                "wrong header size: got {} bytes, expected {} bytes",
                buf.len(),
                HEADER_SIZE
            )));
        }

        Ok(Self {
            timestamp: u32::from_le_bytes(buf[..4].try_into().unwrap()),
            key_size: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            value_size: u32::from_le_bytes(buf[8..].try_into().unwrap()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::random;

    fn assert_header_encode(header: Header) {
        let encoded = header.encode();
        let decoded = Header::decode(&encoded).unwrap();
        assert_eq!(header, decoded);
    }

    #[test]
    fn test_header() {
        let headers = [
            Header {
                timestamp: 10,
                key_size: 10,
                value_size: 10,
            },
            Header {
                timestamp: 0,
                key_size: 0,
                value_size: 0,
            },
            Header {
                timestamp: 10000,
                key_size: 10000,
                value_size: 10000,
            },
        ];

        for header in headers {
            assert_header_encode(header);
        }
    }

    #[test]
    fn test_header_random() {
        for _ in 0..100 {
            let header = Header {
                timestamp: random(),
                key_size: random(),
                value_size: random(),
            };
            assert_header_encode(header);
        }
    }
}
