use std::{
    fs::{File, OpenOptions},
    io::BufWriter,
    path::PathBuf
};

use crate::{
    error::IOError, file_io::write_key_value, value::Value
};

pub struct WriteAheadLog {
    path: PathBuf,
}

impl WriteAheadLog {
    pub fn new(data_dir: &PathBuf, filename: &str) -> Self {
        let mut path: PathBuf = data_dir.clone();
        path.push(filename);

        WriteAheadLog {
            path,
        }
    }

    pub fn write(&mut self, key: &str, value: &Value) -> Result<usize, IOError> {
        let mut writer: BufWriter<File> = match OpenOptions::new().append(true).open(&self.path) {
            Ok(f) => BufWriter::new(f),
            Err(e) => {
                let path_str: &str = self.path.to_str().unwrap();
                return Err(
                    IOError::FailedOpenFile(path_str.to_string(), e.to_string())
                )
            }
        };
        write_key_value(&mut writer, key, value)
    }

    pub fn clear(&mut self) -> Result<(), IOError> {
        match File::create(&self.path) {
            Ok(f) => match f.set_len(0) {
                Ok(_) => Ok(()),
                Err(e) => return Err(IOError::FailedTruncateWAL(e.to_string()))
            },
            Err(e) => {
                let path_str: &str = self.path.to_str().unwrap();
                return Err(
                    IOError::FailedOpenFile(path_str.to_string(), e.to_string())
                )
            }
        }
    }
}