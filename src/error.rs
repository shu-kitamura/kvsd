use std::{error::Error, fmt::{self, Display}, path::PathBuf};

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
    FailedCreateFile(String, String)
}

impl Display for SSTableError {
    fn fmt (&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SSTableError::FailedCreateFile(filename, msg) => write!(f, "SSTableError: Failed to create '{filename}' because the following reason.\n{msg}")
        }
    }
}

impl Error for SSTableError {}

#[derive(Debug, PartialEq)]
pub enum IOError {
    FailedWriteBytes(String),
    FailedOpenFile(PathBuf, String),
    FailedCreateFile(PathBuf, String),
    FailedTruncateWAL(String),
    FailedReadFile(String),
    FailedGetFileSize(PathBuf, String),
    FailedSeek(String)
}

impl Display for IOError {
    fn fmt (&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IOError::FailedWriteBytes(msg) => write!(f, "IOError: Failed to write bytes because the following error is occured.\n{msg}"),
            IOError::FailedOpenFile(path, msg) => write!(f, "IOError: Failed to open '{:?}' because the following error is occurd.\n{}", path, msg),
            IOError::FailedCreateFile(path, msg) => write!(f, "IOError: Failed to create '{:?}' because the following error is occurd.\n{}", path, msg),
            IOError::FailedTruncateWAL(msg) => write!(f, "IOError: Failed to trancate WAL because the following error is occured.\n{msg}"),
            IOError::FailedReadFile(msg) => write!(f, "IOError: Failed to read file because the following error is occured.\n{msg}"),
            IOError::FailedGetFileSize(path, msg) => write!(f, "IOError: Failed to get file size of '{:?}' because the following error is occurd.\n{}", path, msg),
            IOError::FailedSeek(msg) => write!(f, "IOError: Failed to seek file because the following error is occured.\n{msg}")
        }
    }
}

impl Error for IOError {}

#[derive(Debug)]
pub enum ConvertError {
    FailedBytesToValue(String),
    FailedBytesToString(String),
    FailedBytesToUsize(String)
}

impl Display for ConvertError {
    fn fmt (&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConvertError::FailedBytesToValue(msg) => write!(f, "ConvertError: Failed to convert bytes to Value the following error is occured.\n{msg}"),
            ConvertError::FailedBytesToString(msg) => write!(f, "ConvertError: Failed to convert bytes to String the following error is occured.\n{msg}"),
            ConvertError::FailedBytesToUsize(msg) => write!(f, "ConvertError: Failed to convert bytes to usize the following error is occured.\n{msg}")
        }
    }
}

impl Error for ConvertError {}