use std::fmt::{self, Display};

#[derive(Debug, PartialEq)]
pub enum RecordError {
    FailedFromBytes(String)
}

impl Display for RecordError {
    fn fmt (&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecordError::FailedFromBytes(msg) => write!(f, "RecordError : Following error is occured to convert bytes -> record.\n{msg}")
        }
    }
}