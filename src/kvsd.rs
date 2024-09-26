use crate::record::Record;

pub struct KVS {
    memstore: Vec<Record>,
    limit: usize,
    data_dir: String,
}

impl KVS {
    pub fn new() -> Self {
        KVS {
            memstore: Vec::new(),
            limit: 1000,
            data_dir: "".to_string()
        }
    }

    pub fn put(&mut self, key: &str, value: &str, timestamp: i64) {
        let record: Record = Record::new(key, value, timestamp, false);
        self.limit -= record.len();
        self.memstore.push(record);

        if self.limit < 0 {
            // flush
        }

    }

    pub fn get(&mut self, key: &str) -> Option<Record> {
        let contain_key_records = self.memstore.iter().filter(|r| r.key == key);
        let newest_record = contain_key_records.max_by_key(|r|r.timestamp);
        match newest_record {
            Some(r) => Some(r.clone()),
            None => None
        }
    }

    pub fn delete(&mut self, key: &str, timestamp: i64) {
        let record: Record = Record::new(key, "", timestamp, true);
        self.limit = record.len();
        self.memstore.push(record);

        if self.limit < 0 {
            // flush
        }

    }
}
// ----- test -----

#[cfg(test)]
mod tests {
    use std::vec;

    use crate::{
        record::Record,
        kvsd::*
    };
    use chrono::Local;

    #[test]
    fn test_put() {
        let ts = Local::now().timestamp();
        let mut kvs: KVS = KVS::new();
        kvs.put("key", "value", ts);

        assert_eq!(
            kvs.memstore.pop().unwrap(),
            Record::new("key", "value", ts, false)
        );
    }

    #[test]
    fn test_get() {
        let records = vec![
            Record::new("key", "value", 1, false),
            Record::new("key", "value", 10, false),
            Record::new("key", "value", 100, false),
        ];
        let mut kvs: KVS = KVS {
            memstore : records,
            limit: 1000,
            data_dir: "".to_string()
        };

        assert_eq!(
            kvs.get("key").unwrap(),
            Record::new("key","value",100,false),
        );
    }

    #[test]
    fn test_delete() {
        let ts = Local::now().timestamp();
        let mut kvs = KVS::new();
        kvs.delete("key", ts);

        assert_eq!(
            kvs.memstore.pop().unwrap(),
            Record::new("key", "", ts, true)
        );
    }
}