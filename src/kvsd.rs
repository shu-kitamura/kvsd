use crate::record::Record;

pub fn put(records: &mut Vec<Record>, key: &str, value: &str, timestamp: i64) {
    let record: Record = Record::new(key, value, timestamp, false);
    records.push(record);
}

pub fn get(records: &mut Vec<Record>, key: &str) -> Option<Record> {
    let contain_key_records = records.iter().filter(|r| r.key == key);
    let newest_record = contain_key_records.max_by_key(|r|r.timestamp);
    match newest_record {
        Some(r) => Some(r.clone()),
        None => None
    }
}

pub fn delete(records: &mut Vec<Record>, key: &str, timestamp: i64) {
    let record: Record = Record::new(key, "", timestamp, true);
    records.push(record);
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
        let mut records: Vec<Record> = Vec::new();
        put(&mut records, "key", "value", ts);

        assert_eq!(
            records.pop().unwrap(),
            Record::new("key", "value", ts, false)
        );
    }

    #[test]
    fn test_get() {
        let mut records = vec![
            Record::new("key", "value", 1, false),
            Record::new("key", "value", 10, false),
            Record::new("key", "value", 100, false),
        ];

        assert_eq!(
            get(&mut records, "key").unwrap(),
            Record::new("key","value",100,false),
        );
    }

    #[test]
    fn test_delete() {
        let ts = Local::now().timestamp();
        let mut records: Vec<Record> = Vec::new();
        delete(&mut records, "key", ts);

        assert_eq!(
            records.pop().unwrap(),
            Record::new("key", "", ts, true)
        );
    }
}