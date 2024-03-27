mod context;
mod log;
mod reader;
mod writer;

use std::{
    io,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use context::Context;
use crossbeam::queue::ArrayQueue;
use reader::Reader;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{join, sync::broadcast};
use writer::Writer;

pub struct Bitcask {
    handle: Handle,
    shutdown: broadcast::Sender<()>,
}

impl Bitcask {
    fn open() -> Result<Self, Error> {
        todo!()
    }

    fn get_handle(&self) -> Handle {
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
        todo!()
    }
    fn del(&self, key: Bytes) -> Result<bool, Error> {
        todo!()
    }
    fn get(&self, key: Bytes) -> Result<Option<Bytes>, Error> {
        todo!()
    }
    fn merge(&self) -> Result<(), Error> {
        unimplemented!();
    }
    fn sync(&self) -> Result<(), Error> {
        todo!()
    }
    fn close(&self) -> Result<(), Error> {
        unimplemented!();
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("closed!")]
    Closed,
    #[error("I/O error")]
    Io(#[from] io::Error),
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
