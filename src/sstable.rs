use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io::{BufReader, BufWriter},
    path::{Path, PathBuf},
};

use crate::{
    error::{ConvertError, IOError, KVSError},
    file_io::{get_filesize, read_key_value, write_key_value},
    value::Value,
};

#[derive(Debug)]
pub struct SSTable {
    pub data_path: PathBuf,
    index: HashMap<String, usize>,
}

impl SSTable {
    pub fn create(
        data_dir: &Path,
        memtable: &BTreeMap<String, Value>,
        filename: &str,
    ) -> Result<Self, IOError> {
        let mut data_path: PathBuf = data_dir.to_path_buf();
        data_path.push(format!("{filename}.dat"));
        let mut data_writer: BufWriter<File> = get_bufwriter(&data_path)?;

        let mut pointer: usize = 0;
        let mut index: HashMap<String, usize> = HashMap::new();
        for (k, v) in memtable.iter() {
            index.insert(k.to_string(), pointer);
            pointer = write_key_value(&mut data_writer, k, v)?;
        }

        Ok(SSTable {
            data_path,
            index,
        })
    }

    pub fn from_file(path: PathBuf) -> Result<Self, KVSError> {
        let mut buf_reader: BufReader<File> = get_bufreader(&path)?;
        let file_size: usize = get_filesize(&path)?;

        let mut offset: usize = 0;
        let mut index: HashMap<String, usize> = HashMap::new();

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

            let key_len = key.len();
            let value_len = Value::from_bytes(value_bytes)?.len();
            index.insert(key, offset);

            offset += key_len + value_len + 9;
        }

        Ok(SSTable {
            data_path: path,
            index,
        })
    }

    pub fn get(&self, key: &str) -> Result<Option<Value>, KVSError> {
        let pointer = match self.index.get(key) {
            Some(p) => *p,
            None => return Ok(None),
        };

        let mut buf_reader: BufReader<File> = get_bufreader(&self.data_path)?;

        let (_, bytes) = read_key_value(&mut buf_reader, pointer)?;
        let value: Value = Value::from_bytes(bytes)?;

        Ok(Some(value))
    }

    pub fn keys(&self) -> Vec<&String> {
        self.index.keys().collect::<Vec<&String>>()
    }
}

fn get_bufwriter(path: &PathBuf) -> Result<BufWriter<File>, IOError> {
    match File::create(path) {
        Ok(f) => Ok(BufWriter::new(f)),
        Err(e) => Err(IOError::FailedCreateFile(path.clone(), e.to_string())),
    }
}

fn get_bufreader(path: &PathBuf) -> Result<BufReader<File>, IOError> {
    match File::open(path) {
        Ok(f) => Ok(BufReader::new(f)),
        Err(e) => Err(IOError::FailedOpenFile(path.clone(), e.to_string())),
    }
}
