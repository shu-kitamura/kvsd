#[derive(Debug, PartialEq)]
pub struct Record {
    key: String,
    value: String,
    timestamp: i64,
    is_delete: bool
}

impl Record {
    pub fn new(k: &str, v: &str, timestamp: i64, is_delete: bool) -> Self {
        Record {
            key: k.to_string(),
            value: v.to_string(),
            timestamp,
            is_delete
        }
    }

    pub fn to_vec(self) -> Vec<u8> {
        let key = self.key.as_bytes();
        let value = self.value.as_bytes();
        let is_delete = self.is_delete as u8;

        let vec: Vec<u8> = [
            &key.len().to_be_bytes(),
            key,
            &value.len().to_be_bytes(),
            value,
            &self.timestamp.to_be_bytes(),
            &is_delete.to_be_bytes()
        ].concat();

        vec
    }

    pub fn from_vec(vec: Vec<u8>) -> Self {
        let mut start_index: usize = 0;

        // 8バイトまでがキーの長さ
        let key_len: usize = usize::from_be_bytes(
            vec[start_index..8].to_vec().try_into().unwrap()
        );
        start_index = 8;

        // 8 ~ key_len バイトまでがキー
        let key: String = String::from_utf8(vec[start_index..start_index+key_len].to_vec()).unwrap();
        start_index += key_len;

        // key_len ~ 8バイトまでが値の長さ
        let value_len: usize = usize::from_be_bytes(
            vec[start_index..start_index+8].to_vec().try_into().unwrap()
        );
        start_index += 8;

        // key_len+8 ~ value_len バイトまでが値
        let value: String = String::from_utf8(vec[start_index..start_index+value_len].to_vec()).unwrap();
        start_index += value_len;

        // value_len ~ 8バイトまでがタイムスタンプ
        let timestamp: i64 = i64::from_be_bytes(
            vec[start_index..start_index+8].to_vec().try_into().unwrap()
        );
        start_index += 8;

        // 最後1バイトがtrue か false (1ならtrue)
        let is_delete: bool = vec[start_index] == 1;

        Record { key, value, timestamp, is_delete }
    }
}


// ----- test -----

#[cfg(test)]
mod tests {
    use super::Record;
    use chrono::Local;

    #[test]
    fn test_record_new() {
        let ts = Local::now().timestamp();
        let expect = Record {
            key: "Key".to_string(),
            value: "Value".to_string(),
            timestamp: ts,
            is_delete: false
        };

        let actual = Record::new(
            "Key",
            "Value",
            ts,
            false
        );

        assert_eq!(actual, expect);
    }

    #[test]
    fn test_to_vec() {
        // is_delete が true のケース
        let t = Record::new("key", "value", 0, true);
        let v: Vec<u8> = vec![
            0, 0, 0, 0, 0, 0, 0, 3,  // 3 (length of key)
            107, 101, 121,           // key
            0, 0, 0, 0, 0, 0, 0, 5,  // 5 (length of value)
            118, 97, 108, 117, 101,  // value
            0, 0, 0, 0, 0, 0, 0, 0,  // timestamp
            1                        // true
        ];
        assert_eq!(t.to_vec(), v);

        // is_delete が false のケース
        let f = Record::new("key", "value", 0, false);
        let v: Vec<u8> = vec![
            0, 0, 0, 0, 0, 0, 0, 3,  // 3 (length of key)
            107, 101, 121,           // key
            0, 0, 0, 0, 0, 0, 0, 5,  // 5 (length of value)
            118, 97, 108, 117, 101,  // value
            0, 0, 0, 0, 0, 0, 0, 0,  // timestamp
            0                        // false
        ];
        assert_eq!(f.to_vec(), v);
    }

    #[test]
    fn test_from_vec() {
        let t = Record::new("key", "value", 0, true);
        let v: Vec<u8> = vec![
            0, 0, 0, 0, 0, 0, 0, 3,  // 3 (length of key)
            107, 101, 121,           // key
            0, 0, 0, 0, 0, 0, 0, 5,  // 5 (length of value)
            118, 97, 108, 117, 101,  // value
            0, 0, 0, 0, 0, 0, 0, 0,  // timestamp
            1                        // true
        ];
        assert_eq!(Record::from_vec(v), t);

        let f = Record::new("key", "value", 0, false);
        let v: Vec<u8> = vec![
            0, 0, 0, 0, 0, 0, 0, 3,  // 3 (length of key)
            107, 101, 121,           // key
            0, 0, 0, 0, 0, 0, 0, 5,  // 5 (length of value)
            118, 97, 108, 117, 101,  // value
            0, 0, 0, 0, 0, 0, 0, 0,  // timestamp
            0                        // false
        ];
        assert_eq!(Record::from_vec(v), f);
    }
}
