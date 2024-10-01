use std::{
    error::Error,
    fmt::{self, Display},
    path::PathBuf
};

#[derive(Debug)]
pub enum KVSError {
    FailedIO(IOError),
    FailedConvert(ConvertError),
}

impl Display for KVSError {
    fn fmt (&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FailedIO(e) => write!(f, "{}", e.to_string()),
            Self::FailedConvert(e) => write!(f, "{}", e.to_string()),
        }
    }
}

impl Error for KVSError {}

#[derive(Debug, PartialEq)]
pub enum IOError {
    FailedWriteBytes(String),
    FailedOpenFile(PathBuf, String),
    FailedCreateFile(PathBuf, String),
    FailedTruncateWAL(String),
    FailedReadFile(String),
    FailedRemoveFile(PathBuf, String),
    FailedGetFileSize(PathBuf, String),
    FailedGetFilePath(PathBuf, String),
    FailedSeek(String),
    DirectoryNotFound(PathBuf)
}

impl Display for IOError {
    fn fmt (&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IOError::FailedWriteBytes(msg) => write!(f, "IOError: Failed to write bytes because the following error is occured.\n{msg}"),
            IOError::FailedOpenFile(path, msg) => write!(f, "IOError: Failed to open '{:?}' because the following error is occurd.\n{}", path, msg),
            IOError::FailedCreateFile(path, msg) => write!(f, "IOError: Failed to create '{:?}' because the following error is occurd.\n{}", path, msg),
            IOError::FailedTruncateWAL(msg) => write!(f, "IOError: Failed to trancate WAL because the following error is occured.\n{msg}"),
            IOError::FailedReadFile(msg) => write!(f, "IOError: Failed to read file because the following error is occured.\n{msg}"),
            IOError::FailedRemoveFile(path, msg) => write!(f, "IOError: Failed to remove '{:?}' because the following error is occured.\n{}", path, msg),
            IOError::FailedGetFileSize(path, msg) => write!(f, "IOError: Failed to get file size of '{:?}' because the following error is occurd.\n{}", path, msg),
            IOError::FailedGetFilePath(dir_path, msg) => write!(f, "IOError: Failed to get file path in directory '{:?}' because the following error is occured.\n{}", dir_path, msg),
            IOError::FailedSeek(msg) => write!(f, "IOError: Failed to seek file because the following error is occured.\n{msg}"),
            IOError::DirectoryNotFound(path) => write!(f, "IOError: The directory '{:?}' is not found or is not directory.", path)
        }
    }
}

impl From<IOError> for KVSError {
    fn from(value: IOError) -> Self {
        KVSError::FailedIO(value)
    }
}

impl Error for IOError {}

#[derive(Debug)]
pub enum ConvertError {
    FailedBytesToValue(String),
    FailedBytesToString(String),
}

impl Display for ConvertError {
    fn fmt (&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConvertError::FailedBytesToValue(msg) => write!(f, "ConvertError: Failed to convert bytes to Value the following error is occured.\n{msg}"),
            ConvertError::FailedBytesToString(msg) => write!(f, "ConvertError: Failed to convert bytes to String the following error is occured.\n{msg}"),
        }
    }
}

impl Error for ConvertError {}

impl From<ConvertError> for KVSError {
    fn from(value: ConvertError) -> Self {
        KVSError::FailedConvert(value)
    }
}