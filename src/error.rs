use std::{error::Error, fmt::{self, Display}};

#[derive(Debug, PartialEq)]
pub enum ValueError {
    FailedFromBytes(String)
}

impl Display for ValueError {
    fn fmt (&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValueError::FailedFromBytes(msg) => write!(f, "ValueError : Following error is occured to convert bytes -> Value.\n{msg}")
        }
    }
}

impl Error for ValueError {}

#[derive(Debug, PartialEq)]
pub enum SSTableError {
    FailedCreate(String)
}

impl Display for SSTableError {
    fn fmt (&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SSTableError::FailedCreate(msg) => write!(f, "SSTableError: Failed to create SSTable because the following reason.\n{msg}")
        }
    }
}

impl Error for SSTableError {}

#[derive(Debug, PartialEq)]
pub enum IOError {
    FailedWriteBytes(String),
    FailedOpenFile(String, String),
    FailedTruncateWAL(String),
}

impl Display for IOError {
    fn fmt (&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IOError::FailedWriteBytes(msg) => write!(f, "IOError: Failed to write bytes because the following error is occured.\n{msg}"),
            IOError::FailedOpenFile(path, msg) => write!(f, "IOError: Failed to open '{path}' because the following error is occurd.\n{msg}"),
            IOError::FailedTruncateWAL(msg) => write!(f, "IOError: Failed to trancate WAL because the following error is occured.\n{msg}"),
        }
    }
}

impl Error for IOError {}
