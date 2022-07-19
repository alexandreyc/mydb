#[derive(Debug)]
enum Error {
    DecodeError(String),
    KeyTooLong,
    ValueTooLong,
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

#[derive(Debug, PartialEq, Eq)]
struct KeyValue {
    timestamp: u32,
    key: String,
    value: String,
}

impl KeyValue {
    fn new(timestamp: u32, key: String, value: String) -> Result<Self> {
        if key.len() > u32::MAX as usize {
            return Err(Error::KeyTooLong);
        }
        if value.len() > u32::MAX as usize {
            return Err(Error::ValueTooLong);
        }
        Ok(KeyValue {
            timestamp,
            key,
            value,
        })
    }
}

impl Encodable for KeyValue {
    fn encode(&self) -> Vec<u8> {
        let header = Header {
            timestamp: self.timestamp,
            key_size: u32::try_from(self.key.len()).unwrap(), // cannot overflow u32 if we use `KeyValue::new`
            value_size: u32::try_from(self.value.len()).unwrap(), // idem
        };
        let mut buf = header.encode();
        buf.extend_from_slice(self.key.as_bytes());
        buf.extend_from_slice(self.value.as_bytes());
        buf
    }

    fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < HEADER_SIZE {
            return Err(Error::DecodeError(
                "not enough data to decode header".to_string(),
            ));
        }

        let header = Header::decode(&buf[..HEADER_SIZE])?;
        let offset_key = HEADER_SIZE;
        let offset_value = offset_key + (header.key_size as usize);

        let key = &buf[offset_key..offset_value];
        let key = match std::str::from_utf8(key) {
            Ok(key) => key.to_owned(),
            Err(err) => return Err(Error::DecodeError(format!("error decoding key: {}", err))),
        };

        let value = &buf[offset_value..];
        if value.len() != header.value_size as usize {
            return Err(Error::DecodeError(format!(
                "wrong value size: got {} bytes, expected {} bytes",
                value.len(),
                header.value_size
            )));
        }

        let value = match std::str::from_utf8(value) {
            Ok(value) => value.to_owned(),
            Err(err) => return Err(Error::DecodeError(format!("error decoding value: {}", err))),
        };

        Ok(KeyValue {
            timestamp: header.timestamp,
            key,
            value,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::distributions::{Alphanumeric, DistString};
    use rand::random;

    fn assert_header_encode(header: Header) {
        let encoded = header.encode();
        let decoded = Header::decode(&encoded).unwrap();
        assert_eq!(header, decoded);
    }

    fn assert_keyvalue_encode(kv: KeyValue) {
        let encoded = kv.encode();
        let decoded = KeyValue::decode(&encoded).unwrap();
        assert_eq!(kv, decoded);
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

    #[test]
    fn test_keyvalue() {
        let kvs = [
            KeyValue::new(10, "hello".to_string(), "world".to_string()).unwrap(),
            KeyValue::new(0, "".to_string(), "".to_string()).unwrap(),
        ];

        for kv in kvs {
            assert_keyvalue_encode(kv);
        }
    }

    #[test]
    fn test_keyvalue_random() {
        for _ in 0..100 {
            let key_chars = random::<usize>() % (1 << 10);
            let value_chars = random::<usize>() % (1 << 10);

            let key = Alphanumeric.sample_string(&mut rand::thread_rng(), key_chars);
            let value = Alphanumeric.sample_string(&mut rand::thread_rng(), value_chars);

            let kv = KeyValue::new(random(), key, value).unwrap();
            assert_keyvalue_encode(kv);
        }
    }
}
