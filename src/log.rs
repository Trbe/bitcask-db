use std::{
    fs,
    io::{self, Seek, Write},
    num::NonZeroUsize,
    path::Path,
    u64, usize,
};

use bytes::Buf;
use lru::LruCache;
use serde::{de::DeserializeOwned, Serialize};

use crate::{bufio::BufWriterWithPos, utils, Error};

#[derive(Debug, PartialEq, Eq)]
pub(super) struct LogIndex {
    pub(super) len: u64,
    pub(super) pos: u64,
}

#[derive(Debug, Default)]
pub(super) struct LogStatistics {
    live_keys: u64,
    dead_keys: u64,
    dead_bytes: u64,
}

impl LogStatistics {
    pub(super) fn add_live(&mut self) {
        self.live_keys += 1;
    }

    pub(super) fn add_dead(&mut self, nbytes: u64) {
        self.dead_keys += 1;
        self.dead_bytes += nbytes;
    }

    pub(super) fn overwrite(&mut self, nbytes: u64) {
        self.live_keys -= 1;
        self.dead_keys += 1;
        self.dead_bytes += nbytes;
    }

    pub(super) fn live_keys(&self) -> u64 {
        self.live_keys
    }

    pub(super) fn dead_keys(&self) -> u64 {
        self.dead_keys
    }

    pub(super) fn dead_bytes(&self) -> u64 {
        self.dead_bytes
    }

    pub(super) fn fragmentation(&self) -> f64 {
        let live = self.live_keys();
        let dead = self.dead_keys();
        if dead == 0 {
            0.0
        } else {
            let total = dead + live;
            dead as f64 / total as f64
        }
    }
}

pub(super) struct LogDir(LruCache<u64, LogReader>);

impl LogDir {
    pub(super) fn new(size: NonZeroUsize) -> Self {
        Self(LruCache::new(size))
    }

    pub(super) unsafe fn read<T, P>(
        &mut self,
        path: P,
        fileid: u64,
        len: u64,
        pos: u64,
    ) -> Result<T, Error>
    where
        T: DeserializeOwned,
        P: AsRef<Path>,
    {
        match self.0.get_mut(&fileid) {
            Some(reader) => unsafe { reader.at(len, pos) },
            None => {
                let file = open(utils::datafile_name(path, fileid))?;
                let mut reader = LogReader::new(file)?;
                let result = unsafe { reader.at::<T>(len, pos) };
                self.0.put(fileid, reader);
                result
            }
        }
    }

    pub(super) unsafe fn copy<P, W>(
        &mut self,
        path: P,
        fileid: u64,
        len: u64,
        pos: u64,
        writer: &mut W,
    ) -> io::Result<u64>
    where
        P: AsRef<Path>,
        W: Write,
    {
        match self.0.get_mut(&fileid) {
            Some(reader) => unsafe { reader.copy_raw(len, pos, writer) },
            None => {
                let file = open(utils::datafile_name(&path, fileid))?;
                let mut reader = LogReader::new(file)?;
                let result = unsafe { reader.copy_raw(len, pos, writer) };
                self.0.put(fileid, reader);
                result
            }
        }
    }
}

pub(super) struct LogReader {
    mmap: memmap2::Mmap,
    file: fs::File,
}

impl LogReader {
    pub(super) fn new(file: fs::File) -> io::Result<Self> {
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };
        Ok(Self { mmap, file })
    }

    pub(super) unsafe fn at<T>(&mut self, len: u64, pos: u64) -> Result<T, Error>
    where
        T: DeserializeOwned,
    {
        if pos >= self.mmap.len() as u64 {
            self.mmap = unsafe { memmap2::MmapOptions::new().map(&self.file)? };
        }
        let start = pos as usize;
        let end = start + len as usize;
        Ok(bincode::deserialize(&self.mmap[start..end])?)
    }

    pub(super) unsafe fn copy_raw<W>(&mut self, len: u64, pos: u64, dst: &mut W) -> io::Result<u64>
    where
        W: Write,
    {
        if pos >= self.mmap.len() as u64 {
            self.mmap = unsafe { memmap2::MmapOptions::new().map(&self.file)? };
        }
        let start = pos as usize;
        let end = start + len as usize;
        io::copy(&mut self.mmap[start..end].reader(), dst)
    }
}

pub(super) struct LogWriter(BufWriterWithPos<fs::File>);

impl LogWriter {
    pub(super) fn new(file: fs::File) -> io::Result<Self> {
        let writer = BufWriterWithPos::new(file)?;
        Ok(Self(writer))
    }

    pub(super) fn append<T: Serialize>(&mut self, entry: &T) -> Result<LogIndex, Error> {
        let pos = self.0.pos();
        bincode::serialize_into(&mut self.0, entry)?;
        self.0.flush()?;
        let len = self.0.pos() - pos;
        Ok(LogIndex { len, pos })
    }

    pub(super) fn sync(&mut self) -> io::Result<()> {
        self.0.get_ref().sync_all()
    }
}

pub(super) fn create<P: AsRef<Path>>(path: P) -> io::Result<fs::File> {
    fs::OpenOptions::new()
        .append(true)
        .create_new(true)
        .open(path)
}

pub(super) fn open<P: AsRef<Path>>(path: P) -> io::Result<fs::File> {
    fs::OpenOptions::new().read(true).open(path)
}
