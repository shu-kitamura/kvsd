use std::collections::BTreeMap;
use crate::value::Value;

pub struct KVS {
    pub memtable: BTreeMap<String, Value>,
    limit: usize,
}

impl KVS {
    pub fn new() -> Self {
        KVS {
            memtable: BTreeMap::new(),
            limit: 1024,
        }
    }

    pub fn put(&mut self, key: &str, value: &str) {
        self.memtable.insert(key.to_string(), Value::new(value, false));

        if self.memtable.len() >= self.limit {
            self.flush();
        }
    }

    pub fn delete(&mut self, key: &str) {
        self.memtable.insert(key.to_string(), Value::new("", true));

        if self.memtable.len() >= self.limit {
            self.flush();
        }
    }

    pub fn get(&mut self, key: &str) -> Option<&Value> {
        if let Some(v) =  self.memtable.get(key) {
            // get で取得した value の is_delete が true ということは
            // その value は削除されているので None を返す。
            match v.is_delete {
                true => None,
                false => Some(v)
            }
        } else {
            None
        }
    }

    pub fn flush(&mut self) {
        for (k, v) in self.memtable.iter() {
            let key_bytes: Vec<u8> = [&k.len().to_be_bytes(), k.as_bytes()].concat();
            let value_bytes: Vec<u8> = v.clone().to_bytes();
            let bytes: Vec<u8> = [key_bytes, value_bytes].concat();

            // SSTable に write する処理
            // unimplemented!();
        }

        self.memtable.clear();
    }    
}

// ----- test -----
