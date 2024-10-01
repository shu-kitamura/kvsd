use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf
};

use crate::{
    error::{IOError, KVSError},
    sstable::SSTable,
    value::Value,
    wal::WriteAheadLog
};

pub struct KVS {
    memtable: BTreeMap<String, Value>,
    limit: usize,
    data_dir: PathBuf,
    wal: WriteAheadLog,
    sstables: Vec<SSTable>
}

impl KVS {
    pub fn new() -> Result<Self, KVSError> {
        let data_dir: PathBuf = PathBuf::from("./data/");
        if !data_dir.is_dir() {
            return Err(KVSError::FailedIO(IOError::DirectoryNotFound(data_dir)))
        }

        let sstables: Vec<SSTable> = get_sstables(&data_dir)?;
        let mut wal: WriteAheadLog = WriteAheadLog::new(&data_dir, "wal")?;
        let memtable: BTreeMap<String, Value> = wal.recovery()?;

        Ok(KVS {
            memtable,
            limit: 1024,
            wal, 
            data_dir,
            sstables
        })
    }

    pub fn put(&mut self, k: &str, v: &str) -> Result<(), IOError> {
        let value: Value =  Value::new(v, false);
        self.put_key_value(k, value)
    }

    pub fn delete(&mut self, k: &str) -> Result<(), IOError> {
        let value: Value = Value::new("", true);
        self.put_key_value(k, value)
    }

    fn put_key_value(&mut self, key: &str, value: Value) -> Result<(), IOError> {
        self.wal.write(key, &value)?;
        self.memtable.insert(key.to_string(), value);

        if self.limit < self.memtable.len() {
            self.flush()?;
        }

        Ok(())
    }
    
    pub fn get(&mut self, key: &str) -> Result<Option<Value>, KVSError> {
        // memtable からの取得。
        // 取得した value の is_deleted が true の場合、
        // その value は削除されているので None を返す。
        if let Some(value) =  self.memtable.get(key) {
            return match value.is_deleted() {
                true => Ok(None),
                false => Ok(Some(value.clone()))
            }
        }

        // sstable からの取得。
        // memtable と同じく is_deleted が true の場合、None を返す。
        if let Some(value) = self.get_from_sstable(key)? {
            return match value.is_deleted() {
                true => Ok(None),
                false => Ok(Some(value))
            }
        }
        Ok(None)
    }

    fn get_from_sstable(&mut self, key: &str) -> Result<Option<Value>, KVSError> {
        for sstable in self.sstables.iter().rev() {
            if let Some(value) = sstable.get(key)? {
                return Ok(Some(value))
            }
        }
        Ok(None)
    }

    pub fn flush(&mut self) -> Result<(), IOError>{
        let timestamp = chrono::Local::now().timestamp();
        match SSTable::create(&self.data_dir, &self.memtable, &timestamp.to_string()) {
            Ok(sst) => self.sstables.push(sst),
            Err(e) => return Err(e)
        };

        match self.wal.clear() {
            Ok(_) => self.memtable.clear(),
            Err(e) => return Err(e)
        };

        Ok(())
    }
}

fn get_sstables(data_dir: &PathBuf) -> Result<Vec<SSTable>, KVSError> {
    let data_files: Vec<PathBuf> = get_data_files(data_dir)?;
    let mut sstables: Vec<SSTable> = Vec::new();

    for file in data_files {
        let sstable = SSTable::from_file(file)?;
        sstables.push(sstable)
    }
    Ok(sstables)
}

fn get_data_files(data_dir: &PathBuf) -> Result<Vec<PathBuf>, IOError> {
    let files: fs::ReadDir = match fs::read_dir(data_dir) {
        Ok(read_dir) => read_dir,
        Err(e) => return Err(IOError::FailedGetFilePath(data_dir.clone(), e.to_string()))
    };

    let mut data_files: Vec<PathBuf> = Vec::new();

    for result in files {
        let data_file: PathBuf = match result {
            Ok(dir_entry) => dir_entry.path(),
            Err(e) => return Err(IOError::FailedGetFilePath(data_dir.clone(), e.to_string()))
        };

        let mut extention: &str = "";
        if let Some(ext_os_str) = data_file.extension() {
            if let Some(ext_str) = ext_os_str.to_str() {
                extention = ext_str
            }
        };

        if extention == "dat" {
            data_files.push(data_file)
        };
    }
    Ok(data_files)
}
// ----- test -----
