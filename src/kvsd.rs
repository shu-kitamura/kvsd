use std::{
    collections::BTreeMap, error::Error, path::PathBuf
};

use crate::{
    error::IOError, sstable::SSTable, value::Value, wal::WriteAheadLog
};

pub struct KVS {
    pub memtable: BTreeMap<String, Value>,
    limit: usize,
    data_dir: PathBuf,
    pub wal: WriteAheadLog,
    pub sstables: Vec<SSTable>
}

impl KVS {
    pub fn new() -> Result<Self, IOError> {
        let data_dir: PathBuf = PathBuf::from("./data/");
        if !data_dir.is_dir() {
            return Err(IOError::DirectoryNotFound(data_dir))
        }

        let mut wal: WriteAheadLog = match WriteAheadLog::new(&data_dir, "wal") {
            Ok(w) => w,
            Err(e) => return Err(e)
        };

        let memtable: BTreeMap<String, Value> = match wal.recovery() {
            Ok(map) => map,
            Err(e) => return Err(e)
        };

        Ok(KVS {
            memtable,
            limit: 1024,
            wal, 
            data_dir,
            sstables: Vec::new()
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
        if let Err(e) = self.wal.write(key, &value) {
            return Err(e)
        }

        self.memtable.insert(key.to_string(), value);

        if self.limit < self.memtable.len() {
            if let Err(e) = self.flush() {
                return Err(e)
            }
        }

        Ok(())
    }
    
    pub fn get(&mut self, key: &str) -> Result<Option<Value>, IOError> {
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
        match self.get_from_sstable(key) {
            Ok(option) => if let Some(value) = option {
                return match value.is_deleted() {
                    true => Ok(None),
                    false => Ok(Some(value))
                }
            }
            Err(e) => return Err(e)
        }

        Ok(None)
    }

    fn get_from_sstable(&mut self, key: &str) -> Result<Option<Value>, IOError> {
        for sstable in self.sstables.iter() {
            match sstable.get(key) {
                Ok(value) => return Ok(value),
                Err(e) => return Err(e)
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

fn row_len(key: String, value: Value) -> usize {
    key.len() + value.len() + 8
}

// ----- test -----
