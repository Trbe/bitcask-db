use std::{cell::RefCell, sync::Arc};

use bytes::Bytes;

use crate::{context::Context, log::LogDir, DataFileEntry, Error};

#[derive(Debug)]
pub(super) struct Reader {
    ctx: Arc<Context>,
    readers: RefCell<LogDir>,
}

impl Reader {
    pub(super) fn new(ctx: Arc<Context>, readers: RefCell<LogDir>) -> Self {
        Self { ctx, readers }
    }

    pub(super) fn get(&self, key: Bytes) -> Result<Option<Bytes>, Error> {
        match self.ctx.get_keydir().get(&key) {
            Some(keydir_entry) => {
                let datafile_entry = unsafe {
                    self.readers.borrow_mut().read::<DataFileEntry, _>(
                        &self.ctx.path,
                        keydir_entry.value().fileid,
                        keydir_entry.value().len,
                        keydir_entry.value().pos,
                    )?
                };
                Ok(datafile_entry.value)
            }
            None => Ok(None),
        }
    }
}
