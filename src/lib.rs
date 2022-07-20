use std::collections::HashMap;
use std::fs;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::Write;
use std::path::Path;
use std::time;

#[derive(Debug)]
pub enum Error {
    DecodeError(String),
    IoError(io::Error),
    KeyTooLong,
    ValueTooLong,
}

impl From<std::str::Utf8Error> for Error {
    fn from(err: std::str::Utf8Error) -> Self {
        Self::DecodeError(format!("unable to decode utf-8: {}", err))
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::IoError(err)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

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
                "wrong header buffer size: got {} bytes, expected {} bytes",
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

        let Header {
            timestamp,
            key_size,
            value_size,
        } = Header::decode(&buf[..HEADER_SIZE])?;
        let key_size = key_size as usize;
        let value_size = value_size as usize;
        let total_size = HEADER_SIZE + key_size + value_size;

        if buf.len() != total_size {
            return Err(Error::DecodeError(format!(
                "wrong key-value buffer size: got {} bytes, expected {} bytes",
                buf.len(),
                total_size
            )));
        }

        let offset_key = HEADER_SIZE;
        let offset_value = offset_key + key_size;

        let key = &buf[offset_key..offset_value];
        let key = std::str::from_utf8(key)?.to_owned();

        let value = &buf[offset_value..offset_value + value_size];
        let value = std::str::from_utf8(value)?.to_owned();

        Ok(KeyValue {
            timestamp,
            key,
            value,
        })
    }
}

struct KeyDirEntry {
    timestamp: u32,
    size: u32,     // total size of the record (in bytes)
    offset: usize, // offset within the file where the record's header starts
}

struct KeyDir(HashMap<String, KeyDirEntry>);

impl KeyDir {
    fn load<W: io::Read + io::Seek>(w: W) -> Result<Self> {
        let mut buf = vec![0; 1024];
        let mut reader = io::BufReader::new(w);
        let mut keydir = HashMap::new();

        loop {
            let offset = tell(&mut reader)? as usize;
            if let Err(err) = reader.read_exact(&mut buf[..HEADER_SIZE]) {
                if err.kind() == io::ErrorKind::UnexpectedEof {
                    break;
                }
                return Err(Error::IoError(err));
            }

            let Header {
                timestamp,
                value_size,
                key_size,
            } = Header::decode(&buf[..HEADER_SIZE])?;
            let key_size = key_size as usize;
            let value_size = value_size as usize;

            buf.resize(std::cmp::max(key_size, buf.len()), 0);
            reader.read_exact(&mut buf[..key_size])?;
            let key = std::str::from_utf8(&buf[..key_size])?.to_owned();

            reader.seek(io::SeekFrom::Current(value_size as i64))?;

            let entry = KeyDirEntry {
                timestamp,
                size: (HEADER_SIZE + key_size + value_size).try_into().unwrap(),
                offset,
            };

            keydir.insert(key, entry);
        }

        Ok(KeyDir(keydir))
    }
}

pub struct MyDB {
    file: fs::File,
    keydir: KeyDir,
    offset: usize,
}

impl MyDB {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;
        let keydir = KeyDir::load(&file)?;
        Ok(MyDB {
            file,
            keydir,
            offset: 0,
        })
    }

    pub fn new_from_file(file: fs::File) -> Result<Self> {
        let keydir = KeyDir::load(&file)?;
        Ok(MyDB {
            file,
            keydir,
            offset: 0,
        })
    }

    pub fn get(&mut self, key: &str) -> Result<Option<String>> {
        let entry = self.keydir.0.get(key);
        let entry = match entry {
            Some(entry) => entry,
            None => return Ok(None),
        };

        self.file.seek(io::SeekFrom::Start(entry.offset as u64))?;

        let mut kv = vec![0; entry.size as usize];
        self.file.read_exact(&mut kv)?;
        let kv = KeyValue::decode(&kv)?;

        Ok(Some(kv.value))
    }

    pub fn set(&mut self, key: &str, value: &str) -> Result<()> {
        let timestamp = now_timestamp();
        let kv = KeyValue::new(timestamp, key.to_owned(), value.to_owned())?;
        let kv = kv.encode();

        self.file.write_all(&kv)?;
        self.file.flush()?;
        self.file.sync_all()?;

        // TODO: we should first update keydir before writing to file: we can undo keydir update in case
        // file writing has an error.

        let size = kv.len() as u64;
        let entry = KeyDirEntry {
            timestamp,
            size: size.try_into().unwrap(),
            offset: self.offset,
        };
        self.keydir.0.insert(key.to_owned(), entry);
        self.offset += size as usize;

        Ok(())
    }
}

fn now_timestamp() -> u32 {
    let time = time::SystemTime::now()
        .duration_since(time::UNIX_EPOCH)
        .unwrap();
    time.as_secs().try_into().unwrap()
}

fn tell<F: io::Read + io::Seek>(f: &mut F) -> io::Result<u64> {
    f.seek(io::SeekFrom::Current(0))
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
