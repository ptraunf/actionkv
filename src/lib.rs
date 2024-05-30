use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::crc32;
use serde::{Deserialize, Serialize};

type ByteString = Vec<u8>;
type ByteStr = [u8];

#[derive(Debug, Serialize, Deserialize)]
pub struct KeyValuePair {
    pub key: ByteString,
    pub value: ByteString,
}

#[derive(Debug)]
pub struct ActionKV {
    f: File,
    pub index: HashMap<ByteString, u64>,
}

impl ActionKV {
    pub fn open(path: &Path) -> io::Result<Self> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path)?;
        let index = HashMap::new();
        Ok(ActionKV { f, index })
    }

    pub fn load(&mut self) -> io::Result<()> {
        let mut f = BufReader::new(&mut self.f);
        loop {
            let position = f.seek(SeekFrom::Current(0))?;
            let maybe_kv = ActionKV::process_record(&mut f);
            let kv = match maybe_kv {
                Ok(kv) => kv,
                Err(err) => {
                    match err.kind() {
                        io::ErrorKind::UnexpectedEof => {
                            break;
                        }
                        _ => return Err(err)
                    }
                }
            };
            self.index.insert(kv.key, position);
        }
        Ok(())
    }
    fn seek_to_end(&mut self) -> io::Result<u64> {
        self.f.seek(SeekFrom::End(0))
    }
    fn process_record<R: Read>(f: &mut R) -> io::Result<KeyValuePair> {
        let saved_checksum = f.read_u32::<LittleEndian>()?;
        let key_len = f.read_u32::<LittleEndian>()?;
        let val_len = f.read_u32::<LittleEndian>()?;

        let data_len = key_len + val_len;

        let mut data = ByteString::with_capacity(data_len as usize);
        {
            f.by_ref().take(data_len as u64).read_to_end(&mut data)?;
        }
        debug_assert_eq!(data.len(), data_len as usize);
        let checksum = crc32::checksum_ieee(&data);
        if checksum != saved_checksum {
            panic!("data corruption encountered ({:08x} != {:08x})", checksum, saved_checksum);
        }
        let value = data.split_off(key_len as usize);
        let key = data;
        Ok(KeyValuePair { key, value })
    }

    pub fn get(&mut self, key: &ByteStr) -> io::Result<Option<ByteString>> {
        let position = match self.index.get(key) {
            None => return Ok(None),
            Some(position) => *position,
        };

        let kv = self.get_at(position)?;
        Ok(Some(kv.value))
    }
   fn get_at(&mut self, position:u64) -> io::Result<KeyValuePair> {
       let mut f = BufReader::new(&mut self.f);
       f.seek(SeekFrom::Start(position))?;
       let kv = ActionKV::process_record(&mut f)?;
       Ok(kv)
   }

    // fn find(&mut self, target: &ByteStr) -> io::Result<Option<(u64, ByteString)>> {
    //
    // }
    pub fn insert(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()> {
        let position = self.insert_ignore_index(key, value)?;
        self.index.insert(key.to_vec(), position);
        Ok(())
    }

    fn insert_ignore_index(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<u64> {
        let mut f = BufWriter::new(&mut self.f);
        let key_len = key.len();
        let val_len = value.len();
        let mut payload = ByteString::with_capacity(key_len + val_len);
        for byte in key {
            payload.push(*byte);
        }
        for byte in value {
            payload.push(*byte)
        }
        let checksum = crc32::checksum_ieee(&payload);
       let next_byte = SeekFrom::End(0);
        let current_position: u64 = f.seek(SeekFrom::Current(0))?;
        f.seek(next_byte)?;
        f.write_u32::<LittleEndian>(checksum).expect("Failed to write checksum");
        f.write_u32::<LittleEndian>(key_len as u32)?;
        f.write_u32::<LittleEndian>(val_len as u32)?;
        f.write_all(&mut payload)?;

        Ok(current_position)
    }

    #[inline]
    pub fn update(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()> {
        self.insert(key, value)
    }

    #[inline]
    pub fn delete(&mut self, key: &ByteStr) -> io::Result<()> {
        self.insert(key, b"")
    }
}

pub fn about() {
    let about = "

ActionKV is an implementation of Bitcask, a Log-Structured Hash Table

   Fixed-width Header           Variable-width  Variable-width Value
     │                             Key             │
 ┌───┴───────────────────────┐ ┌────┴───────┐ ┌────┴──────┐
 │checksum  key_len  val_len │ │ key        │ │   value   │
  0 1 2 3   4 5 6 7   8 9 A B
 └─┴─┴─┴─┘ └─┴─┴─┴─┘ └─┴─┴─┴─┘ └─┴─┴...─┴─┴─┘ └─┴─┴...┴─┴─┘
   u32       u32      u32      [u8; key_len]  [u8; value_len]

Each KV pair is prefixed by 12 bytes which describe its length (key_len + value_len) and its content (checksum)

    ";

    println!("{}", about);
}