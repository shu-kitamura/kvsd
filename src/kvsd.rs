use std::{
    collections::BTreeMap,
    path::PathBuf
};

use crate::{
    value::Value,
    sstable::SSTable,
    wal::WriteAheadLog,
};

pub struct KVS {
    pub memtable: BTreeMap<String, Value>,
    limit: usize,
    data_dir: PathBuf,
    wal: WriteAheadLog,
    sstables: Vec<SSTable>
}

impl KVS {
    pub fn new() -> Self {
        let data_dir: PathBuf = PathBuf::from("./data/");
        if !data_dir.is_dir() {
            // ディレクトリが無い or ファイルではない というエラーを出したい
            unimplemented!()
        }
        let wal = WriteAheadLog::new(&data_dir, "wal.dat");
        KVS {
            memtable: BTreeMap::new(),
            limit: 1024,
            wal, 
            data_dir,
            sstables: Vec::new()
        }
    }

    pub fn put(&mut self, k: &str, v: &str) {
        let value: Value =  Value::new(v, false);
        self.put_key_value(k, value);
    }

    pub fn delete(&mut self, k: &str) {
        let value = Value::new("", true);
        self.put_key_value(k, value);
    }

    pub fn get(&mut self, key: &str) -> Option<&Value> {
        if let Some(v) =  self.memtable.get(key) {
            // get で取得した value の is_delete が true の場合、
            // その value は削除されているので None を返す。
            match v.is_deleted() {
                true => None,
                false => Some(v)
            }
        } else {
            None
        }
    }

    fn put_key_value(&mut self, key: &str, value: Value) {
        let writed_size = match self.wal.write(key, &value) {
            Ok(size) => size,
            Err(e) => unimplemented!()
        };
        self.memtable.insert(key.to_string(), value);

        if self.limit >= writed_size {
            self.limit -= writed_size
        } else {
            self.flush()
        }
    }

    pub fn flush(&mut self) {
        match SSTable::create(&self.data_dir, &self.memtable){
            Ok(sst) => self.sstables.push(sst),
            Err(e) => unimplemented!()
        };

        match self.wal.clear() {
            Ok(_) => self.memtable.clear(),
            Err(e) => unimplemented!()
        };
    }
}

fn row_len(key: String, value: Value) -> usize {
    key.len() + value.len() + 8
}

// ----- test -----
