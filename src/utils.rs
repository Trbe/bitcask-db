use std::{
    collections::BTreeSet,
    ffi::OsStr,
    fs, io,
    path::{Path, PathBuf},
};

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

pub(super) fn sorted_fileids<P: AsRef<Path>>(path: P) -> io::Result<impl Iterator<Item = u64>> {
    Ok(fs::read_dir(&path)?
        .filter_map(std::result::Result::ok)
        .map(|e| e.path())
        .filter(|p| p.is_file() && p.extension() == Some(OsStr::new(DATAFILE_EXT)))
        .filter_map(|p| {
            p.file_stem()
                .and_then(OsStr::to_str)
                .and_then(|s| s.split('.').next())
                .map(str::parse::<u64>)
        })
        .filter_map(std::result::Result::ok)
        .collect::<BTreeSet<u64>>()
        .into_iter())
}

pub(super) fn timestamp() -> i64 {
    chrono::Local::now()
        .timestamp_nanos_opt()
        .expect("Failed to get timestamp in nanoseconds")
}
