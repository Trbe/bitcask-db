mod bufio;
mod context;
mod log;
mod reader;
mod utils;
mod writer;

use std::{cell::RefCell, collections::HashMap, io, path::Path, sync::Arc};

use bytes::Bytes;
use context::KeyDirEntry;
use crossbeam::{queue::ArrayQueue, utils::Backoff};
use crossbeam_skiplist::SkipMap;
use log::{LogIterator, LogStatistics};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::broadcast;

use crate::log::{LogDir, LogWriter};

use self::{context::Context, reader::Reader, writer::Writer};

const CONCURRENCY: usize = 4;
const READER_CACHE_SIZE: usize = 16;

pub trait KeyValueStorage: Clone + Send + 'static {
    type Error: std::error::Error + Send + Sync;

    fn set(&self, key: Bytes, value: Bytes) -> Result<(), Self::Error>;
    fn get(&self, key: Bytes) -> Result<Option<Bytes>, Self::Error>;
    fn del(&self, key: Bytes) -> Result<bool, Self::Error>;
}

#[allow(dead_code)]
pub struct Bitcask {
    handle: Handle,
    // 
    shutdown: broadcast::Sender<()>,
}

#[allow(dead_code)]
impl Bitcask {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let (keydir, stats, active_fileid) = rebuild_storage(&path)?;

        let ctx = Arc::new(Context::new(&path, keydir));
        let readers = Arc::new(ArrayQueue::new(CONCURRENCY));

        for _ in 0..readers.capacity() {
            readers
                .push(Reader::new(
                    ctx.clone(),
                    RefCell::new(LogDir::new(READER_CACHE_SIZE.try_into().unwrap())),
                ))
                .expect("error");
        }

        let writer = Arc::new(Mutex::new(Writer::new(
            ctx.clone(),
            RefCell::new(LogDir::new(READER_CACHE_SIZE.try_into().unwrap())),
            LogWriter::new(log::create(utils::datafile_name(&path, active_fileid))?)?,
            stats,
            active_fileid,
            0,
        )));

        let handle = Handle {
            ctx,
            writer,
            readers,
        };

        let (shutdown, _) = broadcast::channel(1);
        let bitcask = Self { handle, shutdown };

        Ok(bitcask)
    }

    pub fn get_handle(&self) -> Handle {
        self.handle.clone()
    }
}

impl Drop for Bitcask {
    fn drop(&mut self) {
        self.handle.close();
    }
}

#[derive(Clone, Debug)]
pub struct Handle {
    ctx: Arc<Context>,
    writer: Arc<Mutex<Writer>>,
    readers: Arc<ArrayQueue<Reader>>,
}

impl Handle {
    fn put(&self, key: Bytes, value: Bytes) -> Result<(), Error> {
        if self.ctx.is_closed() {
            return Err(Error::Closed);
        }
        self.writer.lock().put(key, value)
    }

    fn del(&self, key: Bytes) -> Result<bool, Error> {
        if self.ctx.is_closed() {
            return Err(Error::Closed);
        }
        self.writer.lock().delete(key)
    }

    fn get(&self, key: Bytes) -> Result<Option<Bytes>, Error> {
        if self.ctx.is_closed() {
            return Err(Error::Closed);
        }
        let backoff = Backoff::new();
        loop {
            if let Some(reader) = self.readers.pop() {
                let result = reader.get(key);
                self.readers.push(reader).expect("unreachable error");
                break result;
            }
            backoff.spin();
        }
    }

    #[allow(dead_code)]
    fn merge(&self) -> Result<(), Error> {
        unimplemented!();
    }

    #[allow(dead_code)]
    fn sync(&self) -> Result<(), Error> {
        if self.ctx.is_closed() {
            return Err(Error::Closed);
        }
        self.writer.lock().sync()
    }

    fn close(&self) {
        self.ctx.close()
    }
}

impl KeyValueStorage for Handle {
    type Error = Error;

    fn del(&self, key: Bytes) -> Result<bool, Self::Error> {
        self.del(key)
    }

    fn get(&self, key: Bytes) -> Result<Option<Bytes>, Self::Error> {
        self.get(key)
    }

    fn set(&self, key: Bytes, value: Bytes) -> Result<(), Self::Error> {
        self.put(key, value)
    }
}

fn rebuild_storage<P: AsRef<Path>>(
    path: P,
) -> Result<
    (
        SkipMap<Bytes, KeyDirEntry>,
        HashMap<u64, LogStatistics>,
        u64,
    ),
    Error,
> {
    let keydir = SkipMap::default();
    let mut stats = HashMap::default();
    let fileids = utils::sorted_fileids(&path)?;

    let mut active_fileid = None;
    for fileid in fileids {
        match &mut active_fileid {
            None => active_fileid = Some(fileid),
            Some(id) => {
                if fileid > *id {
                    *id = fileid;
                }
            }
        }
        if let Err(e) = populate_keydir_with_hintfile(&path, fileid, &keydir, &mut stats) {
            match e {
                Error::Io(ref ioe) => match ioe.kind() {
                    io::ErrorKind::NotFound => {
                        populate_keydir_with_datafile(&path, fileid, &keydir, &mut stats)?;
                    }
                    _ => return Err(e),
                },
                _ => return Err(e),
            }
        }
    }

    let active_fileid = active_fileid.map(|id| id + 1).unwrap_or_default();
    Ok((keydir, stats, active_fileid))
}

fn populate_keydir_with_hintfile<P>(
    path: P,
    fileid: u64,
    keydir: &SkipMap<Bytes, KeyDirEntry>,
    stats: &mut HashMap<u64, LogStatistics>,
) -> Result<(), Error>
where
    P: AsRef<Path>,
{
    let file = log::open(utils::hintfile_name(&path, fileid))?;
    let mut hintfile_iter = LogIterator::new(file)?;
    while let Some((_, entry)) = hintfile_iter.next::<HintFileEntry>()? {
        let keydir_entry = KeyDirEntry {
            fileid,
            len: entry.len,
            pos: entry.pos,
            tstamp: entry.tstamp,
        };
        stats.entry(fileid).or_default().add_live();
        let prev_entry = keydir.get(&entry.key);
        keydir.insert(entry.key, keydir_entry);
        if let Some(prev_entry) = prev_entry {
            stats
                .entry(prev_entry.value().fileid)
                .or_default()
                .overwrite(prev_entry.value().len);
        }
    }
    Ok(())
}

fn populate_keydir_with_datafile<P>(
    path: P,
    fileid: u64,
    keydir: &SkipMap<Bytes, KeyDirEntry>,
    stats: &mut HashMap<u64, LogStatistics>,
) -> Result<(), Error>
where
    P: AsRef<Path>,
{
    let file = log::open(utils::datafile_name(&path, fileid))?;
    let mut datafile_iter = LogIterator::new(file)?;
    while let Some((datafile_index, datafile_entry)) = datafile_iter.next::<DataFileEntry>()? {
        match datafile_entry.value {
            None => {
                stats
                    .entry(fileid)
                    .or_default()
                    .add_dead(datafile_index.len);
                if let Some(prev_entry) = keydir.remove(&datafile_entry.key) {
                    stats
                        .entry(prev_entry.value().fileid)
                        .or_default()
                        .overwrite(prev_entry.value().len);
                }
            }
            Some(_) => {
                let keydir_entry = KeyDirEntry {
                    fileid,
                    len: datafile_index.len,
                    pos: datafile_index.pos,
                    tstamp: datafile_entry.tstamp,
                };
                stats.entry(fileid).or_default().add_live();
                let prev_entry = keydir.get(&datafile_entry.key);
                keydir.insert(datafile_entry.key, keydir_entry);
                if let Some(prev_entry) = prev_entry {
                    stats
                        .entry(prev_entry.value().fileid)
                        .or_default()
                        .overwrite(prev_entry.value().len);
                }
            }
        }
    }
    Ok(())
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("closed!")]
    Closed,
    #[error("I/O error - {0}")]
    Io(#[from] io::Error),
    #[error("Serialization error - {0}")]
    Serialization(#[from] bincode::Error),
}

#[derive(Serialize, Deserialize, Debug)]
struct HintFileEntry {
    tstamp: i64,
    len: u64,
    pos: u64,
    key: Bytes,
}

#[derive(Serialize, Deserialize, Debug)]
struct DataFileEntry {
    tstamp: i64,
    key: Bytes,
    value: Option<Bytes>,
}
