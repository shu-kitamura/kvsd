use std::{
    collections::BTreeMap, fs::{self, File, OpenOptions}, io::{BufReader, BufWriter}, path::PathBuf
};

use crate::{
    error::{ConvertError, IOError, KVSError},
    file_io::{read_key_value, write_key_value},
    value::Value
};

#[derive(Debug, PartialEq)]
pub struct WriteAheadLog {
    path: PathBuf,
}

impl WriteAheadLog {
    pub fn new(data_dir: &PathBuf, filename: &str) -> Result<Self, IOError> {
        let mut path: PathBuf = data_dir.clone();
        path.push(filename);

        if !path.exists() {
            if let Err(e) = File::create(&path) {
                return Err(IOError::FailedCreateFile(path, e.to_string()))
            }
        }

        Ok(WriteAheadLog {
            path
        })
    }

    pub fn write(&mut self, key: &str, value: &Value) -> Result<usize, IOError> {
        let mut writer: BufWriter<File> = match OpenOptions::new().append(true).open(&self.path) {
            Ok(f) => BufWriter::new(f),
            Err(e) => return Err(
                IOError::FailedOpenFile(self.path.clone(), e.to_string())
            )
        };
        write_key_value(&mut writer, key, value)
    }

    pub fn clear(&mut self) -> Result<(), IOError> {
        match File::create(&self.path) {
            Ok(f) => match f.set_len(0) {
                Ok(_) => Ok(()),
                Err(e) => return Err(IOError::FailedTruncateWAL(e.to_string()))
            },
            Err(e) => return Err(
                IOError::FailedOpenFile(self.path.clone(), e.to_string())
            )
        }
    }

    pub fn recovery(&mut self) -> Result<BTreeMap<String, Value>, KVSError> {
        let mut buf_reader: BufReader<File> = match File::open(&self.path) {
            Ok(f) => BufReader::new(f),
            Err(e) => return Err(KVSError::FailedIO(
                    IOError::FailedOpenFile(self.path.clone(), e.to_string())
            ))
        };

        let file_size: usize = match fs::metadata(&self.path) {
            Ok(metadata) => metadata.len() as usize,
            Err(e) => return Err(KVSError::FailedIO(
                IOError::FailedGetFileSize(self.path.clone(), e.to_string())
            ))
        };

        let mut offset: usize = 0;
        let mut btm: BTreeMap<String, Value> = BTreeMap::new();
        while offset < file_size {
            let (key_bytes, value_bytes) = read_key_value(&mut buf_reader, offset)?;

            let key: String = match String::from_utf8(key_bytes) {
                Ok(s) => s,
                Err(e) => return Err(KVSError::FailedConvert(
                    ConvertError::FailedBytesToString(e.to_string())
                ))
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
            path: PathBuf::from("data/wal")
        };
        assert_eq!(WriteAheadLog::new(&path, "wal").unwrap(), wal);
    }
}