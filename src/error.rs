use std::{
    error::Error,
    fmt::{self, Display},
    path::PathBuf,
};

#[derive(Debug)]
pub enum KVSError {
    FailedIO(IOError),
    FailedConvert(ConvertError),
}

impl Display for KVSError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FailedIO(e) => write!(f, "{e}"),
            Self::FailedConvert(e) => write!(f, "{e}"),
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
    DirectoryNotFound(PathBuf),
}

impl Display for IOError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IOError::FailedWriteBytes(msg) => write!(f, "IOError: Failed to write bytes because the following error occurred.\n{msg}"),
            IOError::FailedOpenFile(path, msg) => write!(f, "IOError: Failed to open '{path:?}' because the following error occurred.\n{msg}"),
            IOError::FailedCreateFile(path, msg) => write!(f, "IOError: Failed to create '{path:?}' because the following error occurred.\n{msg}"),
            IOError::FailedTruncateWAL(msg) => write!(f, "IOError: Failed to truncate WAL because the following error occurred.\n{msg}"),
            IOError::FailedReadFile(msg) => write!(f, "IOError: Failed to read file because the following error occurred.\n{msg}"),
            IOError::FailedRemoveFile(path, msg) => write!(f, "IOError: Failed to remove '{path:?}' because the following error occurred.\n{msg}"),
            IOError::FailedGetFileSize(path, msg) => write!(f, "IOError: Failed to get file size of '{path:?}' because the following error occurred.\n{msg}"),
            IOError::FailedGetFilePath(dir_path, msg) => write!(f, "IOError: Failed to get file path in directory '{dir_path:?}' because the following error occurred.\n{msg}"),
            IOError::FailedSeek(msg) => write!(f, "IOError: Failed to seek file because the following error occurred.\n{msg}"),
            IOError::DirectoryNotFound(path) => write!(f, "IOError: The directory '{path:?}' is not found or is not directory.")
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConvertError::FailedBytesToValue(msg) => write!(f, "ConvertError: Failed to convert bytes to Value because the following error occurred.\n{msg}"),
            ConvertError::FailedBytesToString(msg) => write!(f, "ConvertError: Failed to convert bytes to String because the following error occurred.\n{msg}"),
        }
    }
}

impl Error for ConvertError {}

impl From<ConvertError> for KVSError {
    fn from(value: ConvertError) -> Self {
        KVSError::FailedConvert(value)
    }
}
