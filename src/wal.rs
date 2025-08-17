use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter},
    path::{Path, PathBuf},
};

use crate::{
    error::{ConvertError, IOError, KVSError},
    file_io::{get_filesize, read_key_value, write_key_value},
    value::Value,
};

/// Represents a Write-Ahead Log (WAL).
#[derive(Debug, PartialEq)]
pub struct WriteAheadLog {
    /// The path to the WAL file.
    path: PathBuf,
}

impl WriteAheadLog {
    /// Creates a new `WriteAheadLog`.
    ///
    /// # Arguments
    ///
    /// * `data_dir` - The directory to store the WAL file in.
    /// * `filename` - The name of the WAL file.
    pub fn new(data_dir: &Path, filename: &str) -> Result<Self, IOError> {
        let mut path: PathBuf = data_dir.to_path_buf();
        path.push(filename);

        if !path.exists() {
            if let Err(e) = File::create(&path) {
                return Err(IOError::FailedCreateFile(path, e.to_string()));
            }
        }

        Ok(WriteAheadLog { path })
    }

    /// Writes a key-value pair to the WAL.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to write.
    /// * `value` - The value to write.
    pub fn write(&mut self, key: &str, value: &Value) -> Result<usize, IOError> {
        let mut writer: BufWriter<File> = match OpenOptions::new().append(true).open(&self.path) {
            Ok(f) => BufWriter::new(f),
            Err(e) => return Err(IOError::FailedOpenFile(self.path.clone(), e.to_string())),
        };
        write_key_value(&mut writer, key, value)
    }

    /// Clears the WAL.
    pub fn clear(&mut self) -> Result<(), IOError> {
        match File::create(&self.path) {
            Ok(f) => match f.set_len(0) {
                Ok(_) => Ok(()),
                Err(e) => Err(IOError::FailedTruncateWAL(e.to_string())),
            },
            Err(e) => Err(IOError::FailedOpenFile(self.path.clone(), e.to_string())),
        }
    }

    /// Recovers the memtable from the WAL.
    pub fn recovery(&mut self) -> Result<BTreeMap<String, Value>, KVSError> {
        let mut buf_reader: BufReader<File> = match File::open(&self.path) {
            Ok(f) => BufReader::new(f),
            Err(e) => {
                return Err(KVSError::FailedIO(IOError::FailedOpenFile(
                    self.path.clone(),
                    e.to_string(),
                )))
            }
        };

        let file_size: usize = get_filesize(&self.path)?;

        let mut offset: usize = 0;
        let mut btm: BTreeMap<String, Value> = BTreeMap::new();
        while offset < file_size {
            let (key_bytes, value_bytes) = read_key_value(&mut buf_reader, offset)?;

            let key: String = match String::from_utf8(key_bytes) {
                Ok(s) => s,
                Err(e) => {
                    return Err(KVSError::FailedConvert(ConvertError::FailedBytesToString(
                        e.to_string(),
                    )))
                }
            };

            let value: Value = Value::from_bytes(value_bytes)?;

            offset += key.len() + value.len() + 9;
            btm.insert(key, value);
        }

        Ok(btm)
    }
}

// ----- test -----

#[cfg(test)]
mod tests {
    use crate::wal::*;

    #[test]
    fn test_wal_new() {
        let path: PathBuf = PathBuf::from("data");
        let wal = WriteAheadLog {
            path: PathBuf::from("data/wal"),
        };
        assert_eq!(WriteAheadLog::new(&path, "wal").unwrap(), wal);
    }
}
