use std::path::{Path, PathBuf};

const DATAFILE_EXT: &str = "data";

const HINTFILE_EXT: &str = "hint";

pub(super) fn datafile_name<P: AsRef<Path>>(path: P, fileid: u64) -> PathBuf {
    path.as_ref()
        .join(format!("{fileid}.bitcask.{DATAFILE_EXT}"))
}

pub(super) fn hintfile_name<P: AsRef<Path>>(path: P, fileid: u64) -> PathBuf {
    path.as_ref()
        .join(format!("{fileid}.bitcask.{HINTFILE_EXT}"))
}

pub(super) fn timestamp() -> i64 {
    chrono::Local::now()
        .timestamp_nanos_opt()
        .expect("Failed to get timestamp in nanoseconds")
}
