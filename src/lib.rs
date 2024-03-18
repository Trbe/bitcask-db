use std::{collections::HashMap, path::Path};

use bytes::Bytes;
use lru::LruCache;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct DataFileEntry {
    tstamp: i64,
    key: Bytes,
    value: Option<Bytes>,
}

struct LogWriter;

struct LogIndex {
    len: u64,
    pos: u64,
}

impl LogWriter {
    fn append<T: Serialize>(&mut self, entry: &T) -> Result<LogIndex, Error> {
        todo!()
    }
}

#[derive(Debug)]
struct LogReader {}

impl LogReader {
    unsafe fn at<T: DeserializeOwned>(&mut self, len: u64, pos: u64) -> Result<T, Error> {
        todo!()
    }
}

struct LogDir(LruCache<u64, LogReader>);

impl LogDir {
    unsafe fn read<T, P>(&mut self, path: P, fileid: u64, len: u64, pos: u64) -> Result<T, Error>
    where
        T: DeserializeOwned,
        P: AsRef<Path>,
    {
        unimplemented!();
    }
}

type KeyDir = HashMap<Bytes, KeyDirEntry>;

struct KeyDirEntry {
    fileid: u64,
    len: u64,
    pos: u64,
    tstamp: i64,
}
