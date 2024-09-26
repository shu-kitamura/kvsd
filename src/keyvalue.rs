pub struct Key {
    key: String,
    length: usize
}

impl Key {
    pub fn new(string: &str) -> Self {
        Key { key: string.to_string(), length: string.len() }
    }

    pub fn to_bytes(self) -> Vec<u8> {
        let key = self.key.as_bytes();
        let len = self.length.to_be_bytes();
        [&len, key].concat()
    }
}

pub struct Value {
    value: String,
    length: usize,
    is_delete: bool,
}

impl Value {
    pub fn new(string: &str, ts:i64, is_del: bool) -> Self {
        Value {
            value: string.to_string(),
            length: string.len(),
            is_delete: is_del,
        }
    }

    pub fn to_bytes(self) -> Vec<u8> {
        let value = self.value.as_bytes();
        let value_len = self.length.to_be_bytes();
        let is_del = u8::from(self.is_delete).to_be_bytes();
        [&value_len, value, &is_del].concat()
    }
}