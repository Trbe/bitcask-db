use std::{cell::RefCell, collections::HashMap, sync::Arc};

use bytes::Bytes;
use tracing::debug;

use crate::{
    context::{Context, KeyDirEntry}, log::{LogDir, LogStatistics, LogWriter}, utils, DataFileEntry, Error
};

#[derive(Debug)]
pub(super) struct Writer {
    ctx: Arc<Context>,
    readers: RefCell<LogDir>,
    writer: LogWriter,
    stats: HashMap<u64, LogStatistics>,
    active_fileid: u64,
    written_bytes: u64,
}

impl Writer {
    pub(super) fn new(
        ctx: Arc<Context>,
        readers: RefCell<LogDir>,
        writer: LogWriter,
        stats: HashMap<u64, LogStatistics>,
        active_fileid: u64,
        written_bytes: u64,
    ) -> Self {
        Self {
            ctx,
            readers,
            writer,
            stats,
            active_fileid,
            written_bytes,
        }
    }

    pub(super) fn put(&mut self, key: Bytes, value: Bytes) -> Result<(), Error> {
        let keydir_entry = self.write(utils::timestamp())
    }


    fn write(
        &mut self,
        tstamp: i64,
        key: Bytes,
        value: Option<Bytes>
    ) -> Result<KeyDirEntry, Error>{

        let datafile_entry = DataFileEntry {tstamp, key, value};
        let index = self.writer.append(&datafile_entry)?;
        self.written_bytes += index.len;
        {
            let entry = self.stats.entry(self.active_fileid).or_default();
            if datafile_entry.value.is_some() {
                entry.add_live();
            } else {
                entry.add_dead(index.len);
            }

        }

        let keydir_entry = KeyDirEntry {
            fileid: self.active_fileid,
            len: index.len,
            pos: index.pos,
            tstamp,
        };

        if self.written_bytes > 4096 {
        }

        Ok(keydir_entry)
    }
}
