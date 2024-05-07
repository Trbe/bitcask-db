mod bufio;
mod context;
mod log;
mod reader;
mod utils;
mod writer;

use std::{io, sync::Arc};

use bytes::Bytes;
use crossbeam::{queue::ArrayQueue, utils::Backoff};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{join, sync::broadcast};

use self::{context::Context, reader::Reader, writer::Writer};

pub trait KeyValueStorage: Clone + Send + 'static {
    type Error: std::error::Error + Send + Sync;

    fn set(&self, key: Bytes, value: Bytes) -> Result<(), Self::Error>;
    fn get(&self, key: Bytes) -> Result<Option<Bytes>, Self::Error>;
    fn del(&self, key: Bytes) -> Result<bool, Self::Error>;
}

#[allow(dead_code)]
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
