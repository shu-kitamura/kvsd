use std::{error::Error, fmt::{self, Display}};

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

impl Error for RecordError {}

#[derive(Debug, PartialEq)]
pub enum SSTableError {
    FailedCreate(String)
}

impl Display for SSTableError {
    fn fmt (&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SSTableError::FailedCreate(msg) => write!(f, "SSTableError : Failed to create SSTable because the following reason.\n{msg}")
        }
    }
}

impl Error for SSTableError {}