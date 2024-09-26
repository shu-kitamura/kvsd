use crate::error::ValueError;

#[derive(Debug, PartialEq)]
pub struct Value {
    value: String,
    length: usize,
    pub is_delete: bool,
}

impl Value {
    pub fn new(string: &str, is_del: bool) -> Self {
        Value {
            value: string.to_string(),
            length: string.len(),
            is_delete: is_del,
        }
    }

    pub fn len(&self) -> usize {
        self.length + 9
    }

    pub fn to_bytes(self) -> Vec<u8> {
        let value = self.value.as_bytes();
        let value_len = self.length.to_be_bytes();
        let is_del = u8::from(self.is_delete).to_be_bytes();
        [&value_len, value, &is_del].concat()
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, ValueError> {
        let mut start_index: usize = 0;

        // 8バイトまでが value の長さ
        let length: usize = match bytes[start_index..8].to_vec().try_into() {
                Ok(bytes) => usize::from_be_bytes(bytes),
                Err(v) => return Err(ValueError::FailedFromBytes(
                    format!("failed to convert value length. To try the following vec to usize {:?}", v)
                ))
            };
        start_index = 8;

        // 8 ~ length バイトまでが value 本体
        let value: String = match String::from_utf8(bytes[start_index..start_index+length].to_vec()){
            Ok(s) => s,
            Err(e) => return Err(ValueError::FailedFromBytes(e.to_string()))
        };
        start_index += length;

        // 最後1バイトがtrue か false (1ならtrue)
        let is_delete: bool = match bytes[start_index] {
            0 => false,
            1 => true,
            _ => return Err(ValueError::FailedFromBytes(
                format!("Invalid value '{}' is read. is_delete expect '0' or '1'", bytes[start_index])
            ))
        };

        Ok(Value { value, length, is_delete })

    }
}

// ----- test -----

#[cfg(test)]
mod tests {
    use crate::value::*;

    #[test]
    fn test_value_new() {
        let t = Value {
            value: "value".to_string(),
            length: 5,
            is_delete: true
        };
        assert_eq!(Value::new("value", true), t);

        let f = Value {
            value: "value".to_string(),
            length: 5,
            is_delete: false
        };
        assert_eq!(Value::new("value", false), f);
    }

    #[test]
    fn test_len() {
        let value = Value::new("value", false);
        assert_eq!(value.len(), 14);
    }

    #[test]
    fn test_to_bytes() {
        let v: Vec<u8> = vec![
            0, 0, 0, 0, 0, 0, 0, 5,  // 5 (length of value)
            118, 97, 108, 117, 101,  // value
            1                        // true
        ];
        let t = Value::new("value", true);
        assert_eq!(t.to_bytes(), v);

        let v: Vec<u8> = vec![
            0, 0, 0, 0, 0, 0, 0, 5,  // 5 (length of value)
            118, 97, 108, 117, 101,  // value
            0                        // false
        ];
        let t = Value::new("value", false);
        assert_eq!(t.to_bytes(), v);
    }

    #[test]
    fn test_from_bytes() {
        let t = Value::new("test", true);
        let bytes: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 1];
        assert_eq!(Value::from_bytes(bytes).unwrap(), t);

        let f = Value::new("test", false);
        let bytes: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 0];
        assert_eq!(Value::from_bytes(bytes).unwrap(), f);
        
    }
}