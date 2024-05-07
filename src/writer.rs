use std::{cell::RefCell, collections::HashMap, fs, sync::Arc};

use bytes::Bytes;

use crate::{
    context::{Context, KeyDirEntry},
    log::{self, LogDir, LogStatistics, LogWriter},
    utils, DataFileEntry, Error,
};

const MAX_FILE_SIZE: u64 = 16 * 1024 * 1024;

#[allow(dead_code)]
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
        let keydir_entry = self.write(utils::timestamp(), key.clone(), Some(value))?;

        if let Some(prev_entry) = self.ctx.keydir_set(key, keydir_entry) {
            self.stats
                .entry(prev_entry.value().fileid)
                .or_default()
                .overwrite(prev_entry.value().len);
        }

        Ok(())
    }

    pub(super) fn delete(&mut self, key: Bytes) -> Result<bool, Error> {
        self.write(utils::timestamp(), key.clone(), None)?;

        match self.ctx.get_keydir().remove(&key) {
            Some(prev_entry) => {
                self.stats
                    .entry(prev_entry.value().fileid)
                    .or_default()
                    .overwrite(prev_entry.value().len);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn write(
        &mut self,
        tstamp: i64,
        key: Bytes,
        value: Option<Bytes>,
    ) -> Result<KeyDirEntry, Error> {
        let datafile_entry = DataFileEntry { tstamp, key, value };
        let index = self.writer.append(&datafile_entry)?;
        //self.writer.sync()?;
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

        if self.written_bytes > MAX_FILE_SIZE {
            self.new_active_datafile(self.active_fileid + 1)?;
        }

        Ok(keydir_entry)
    }

    pub(super) fn sync(&mut self) -> Result<(), Error> {
        self.writer.sync()?;
        Ok(())
    }

    #[allow(dead_code)]
    pub(super) fn get_stats(&self) -> &HashMap<u64, LogStatistics> {
        &self.stats
    }

    fn new_active_datafile(&mut self, fileid: u64) -> Result<(), Error> {
        self.active_fileid = fileid;
        self.writer = LogWriter::new(log::create(utils::datafile_name(
            self.ctx.path.as_path(),
            self.active_fileid,
        ))?)?;
        self.written_bytes = 0;
        Ok(())
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        if self.written_bytes != 0 {
            return;
        }
        let active_datafile = utils::datafile_name(&self.ctx.path, self.active_fileid);
        if let Err(e) = fs::remove_file(active_datafile) {
            println!("{e}aaa");
        }
    }
}
