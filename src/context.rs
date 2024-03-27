use bytes::Bytes;
use crossbeam::atomic::AtomicCell;
use crossbeam_skiplist::SkipMap;

#[derive(Debug)]
pub(super) struct Context {
    keydir: SkipMap<Bytes, KeyDirEntry>,
    closed: AtomicCell<bool>,
}

#[derive(Debug)]
pub(super) struct KeyDirEntry {
    pub(super) fileid: u64,
    pub(super) len: u64,
    pub(super) pos: u64,
    pub(super) tstamp: i64,
}
