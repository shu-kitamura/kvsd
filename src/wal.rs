use std::{
    collections::BTreeMap, fs::{self, File, OpenOptions}, io::{BufReader, BufWriter}, path::PathBuf
};

use crate::{
    error::IOError,
    file_io::{read_key_value, write_key_value},
    value::Value
};

#[derive(Debug, PartialEq)]
pub struct WriteAheadLog {
    path: PathBuf,
}

impl WriteAheadLog {
    pub fn new(data_dir: &PathBuf, filename: &str) -> Self {
        let mut path: PathBuf = data_dir.clone();
        path.push(filename);

        if !path.exists() {
            if let Err(e) = File::create(&path) {
                unimplemented!()
            };
        }

        WriteAheadLog {
            path
        }
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

    pub fn recovery(&mut self) -> Result<BTreeMap<String, Value>, IOError> {
        let mut buf_reader: BufReader<File> = match File::open(&self.path) {
            Ok(f) => BufReader::new(f),
            Err(e) => return Err(
                IOError::FailedOpenFile(self.path.clone(), e.to_string())
            )
        };

        let file_size: usize = match fs::metadata(&self.path) {
            Ok(metadata) => metadata.len() as usize,
            Err(e) => unimplemented!()
        };

        let mut offset: usize = 0;
        let mut btm: BTreeMap<String, Value> = BTreeMap::new();
        while offset < file_size {
            let (k, v) = read_key_value(&mut buf_reader, offset);
            offset += k.len() + v.len() + 9;
            btm.insert(k, v);
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
        let path: PathBuf = PathBuf::from("test");
        let wal = WriteAheadLog {
            path: PathBuf::from("test/wal")
        };
        assert_eq!(WriteAheadLog::new(&path, "wal"), wal);
    }
}