use std::path::{Path, PathBuf};

use bytes::Bytes;
use crossbeam::atomic::AtomicCell;
use crossbeam_skiplist::{map::Entry, SkipMap};

#[derive(Debug)]
pub(super) struct Context {
    pub path: PathBuf,
    keydir: SkipMap<Bytes, KeyDirEntry>,
    closed: AtomicCell<bool>,
}

impl Context {
    pub(super) fn new<P: AsRef<Path>>(path: P, keydir: SkipMap<Bytes, KeyDirEntry>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            keydir,
            closed: AtomicCell::new(false),
        }
    }

    pub(super) fn keydir_set(
        &self,
        key: Bytes,
        keydir_entry: KeyDirEntry,
    ) -> Option<Entry<'_, Bytes, KeyDirEntry>> {
        let prev_entry = self.keydir.get(&key);
        self.keydir.insert(key, keydir_entry);
        prev_entry
    }

    pub(super) fn get_keydir(&self) -> &SkipMap<Bytes, KeyDirEntry> {
        &self.keydir
    }

    pub(super) fn close(&self) {
        self.closed.store(true)
    }

    pub(super) fn is_closed(&self) -> bool {
        self.closed.load()
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub(super) struct KeyDirEntry {
    pub(super) fileid: u64,
    pub(super) len: u64,
    pub(super) pos: u64,
    pub(super) tstamp: i64,
}
