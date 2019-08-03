use std::sync::Arc;

use engine::rocks::{SstWriter, SstWriterBuilder};
use engine::{CF_DEFAULT, CF_WRITE, DB};
use kvproto::backup::File;
use tikv::storage::txn::TxnEntry;
use tikv_util;

use crate::{Error, Result, Storage};

pub struct BackupWriter {
    name: String,
    default: SstWriter,
    default_written: bool,
    write: SstWriter,
    write_written: bool,
}

impl BackupWriter {
    pub fn new(db: Arc<DB>, name: &str) -> Result<BackupWriter> {
        let default = SstWriterBuilder::new()
            .set_in_memory(true)
            .set_cf(CF_DEFAULT)
            .set_db(db.clone())
            .build(name)?;
        let write = SstWriterBuilder::new()
            .set_in_memory(true)
            .set_cf(CF_WRITE)
            .set_db(db.clone())
            .build(name)?;
        let name = name.to_owned();
        Ok(BackupWriter {
            name,
            default,
            default_written: false,
            write,
            write_written: false,
        })
    }

    pub fn write<I>(&mut self, entris: I) -> Result<()>
    where
        I: Iterator<Item = TxnEntry>,
    {
        for e in entris {
            match e {
                TxnEntry::Commit { default, write } => {
                    self.default.put(&default.0, &default.1)?;
                    self.default_written = true;
                    self.write.put(&write.0, &write.1)?;
                    self.write_written = true;
                }
                TxnEntry::Prewrite { .. } | TxnEntry::Rollback { .. } => {
                    return Err(Error::Other(
                        "prewrite and rollback is not supported".into(),
                    ))
                }
            }
        }
        Ok(())
    }

    pub fn save(mut self, storage: &dyn Storage) -> Result<Vec<File>> {
        let name = self.name;
        let save_and_build_file = |cf, mut contents: &[u8]| -> Result<File> {
            let name = format!("{}_{}", name, cf);
            let checksum = tikv_util::file::calc_crc32_bytes(contents);
            storage.write(&name, &mut contents as &mut dyn std::io::Read)?;
            let mut file = File::new();
            file.set_crc32(checksum);
            file.set_name(name);
            Ok(file)
        };
        let mut files = Vec::with_capacity(2);
        let mut buf = Vec::new();
        if self.default_written {
            // Save default cf contents.
            buf.reserve(self.default.file_size() as _);
            self.default.finish_into(&mut buf)?;
            let default = save_and_build_file(CF_DEFAULT, &mut buf)?;
            files.push(default);
            buf.clear();
        }
        if self.write_written {
            // Save write cf contents.
            buf.reserve(self.write.file_size() as _);
            self.write.finish_into(&mut buf)?;
            let write = save_and_build_file(CF_WRITE, &mut buf)?;
            files.push(write);
        }
        Ok(files)
    }
}
