use std::collections::HashMap;

use crate::value::Value;

pub struct KVS {
    pub memtable: HashMap<String, Value>,
    limit: usize,
}

impl KVS {
    pub fn new() -> Self {
        KVS {
            memtable: HashMap::new(),
            limit: 1024,
        }
    }

    pub fn put(&mut self, key: &str, value: &str) {
        self.memtable.insert(key.to_string(), Value::new(value, false));

        if self.memtable.len() >= self.limit {
            // flush 処理
        }
    }

    pub fn delete(&mut self, key: &str) {
        self.memtable.insert(key.to_string(), Value::new("", true));

        if self.memtable.len() >= self.limit {
            // flush 処理
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
}

// ----- test -----
