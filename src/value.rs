use crate::error::ConvertError;
use std::fmt::{self, Display};

#[derive(Debug, PartialEq, Clone)]
pub struct Value {
    value: String,
    is_delete: bool,
}

impl Value {
    pub fn new(string: &str, is_del: bool) -> Self {
        Value {
            value: string.to_string(),
            is_delete: is_del,
        }
    }

    pub fn len(&self) -> usize {
        self.value.len() + 9
    }

    pub fn is_deleted(&self) -> bool {
        self.is_delete
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let value = self.value.as_bytes();
        let value_len = (self.value.len() + 1).to_be_bytes();
        let is_del = u8::from(self.is_delete).to_be_bytes();
        [&value_len, value, &is_del].concat()
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, ConvertError> {
        // 0 ~ length-1 までが value 本体
        let value: String = match String::from_utf8(bytes[0..bytes.len() - 1].to_vec()) {
            Ok(s) => s,
            Err(e) => return Err(ConvertError::FailedBytesToValue(e.to_string())),
        };

        // 最後1バイトがtrue か false (1ならtrue)
        let is_delete: bool = match bytes[bytes.len() - 1] {
            0 => false,
            1 => true,
            _ => {
                return Err(ConvertError::FailedBytesToValue(format!(
                    "Invalid value '{}' is read. is_delete expect '0' or '1'",
                    bytes[bytes.len() - 1]
                )))
            }
        };

        Ok(Value { value, is_delete })
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
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
            is_delete: true,
        };
        assert_eq!(Value::new("value", true), t);

        let f = Value {
            value: "value".to_string(),
            is_delete: false,
        };
        assert_eq!(Value::new("value", false), f);
    }

    #[test]
    fn test_len() {
        let value = Value::new("value", false);
        assert_eq!(value.len(), 14);
    }

    #[test]
    fn test_is_deleted() {
        let t = Value::new("v", true);
        assert!(t.is_deleted());

        let f = Value::new("v", false);
        assert!(!f.is_deleted());
    }

    #[test]
    fn test_to_bytes() {
        let v: Vec<u8> = vec![
            0, 0, 0, 0, 0, 0, 0, 6, // 6 (length of value)
            118, 97, 108, 117, 101, // value
            1,   // true
        ];
        let t = Value::new("value", true);
        assert_eq!(t.to_bytes(), v);

        let v: Vec<u8> = vec![
            0, 0, 0, 0, 0, 0, 0, 6, // 6 (length of value)
            118, 97, 108, 117, 101, // value
            0,   // false
        ];
        let t = Value::new("value", false);
        assert_eq!(t.to_bytes(), v);
    }

    #[test]
    fn test_from_bytes() {
        let t = Value::new("test", true);
        let bytes: Vec<u8> = vec![116, 101, 115, 116, 1];
        assert_eq!(Value::from_bytes(bytes).unwrap(), t);

        let f = Value::new("test", false);
        let bytes: Vec<u8> = vec![116, 101, 115, 116, 0];
        assert_eq!(Value::from_bytes(bytes).unwrap(), f);
    }

    #[test]
    fn test_display() {
        let str_true = "test_true";
        let t = Value::new(str_true, true);
        assert_eq!(format!("{t}"), String::from(str_true));

        let str_false = "test_false";
        let f = Value::new(str_false, true);
        assert_eq!(format!("{f}"), String::from(str_false));
    }
}
