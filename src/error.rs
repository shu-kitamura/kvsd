use std::{
    error::Error,
    fmt::{self, Display},
    path::PathBuf,
};

/// The main error type for the KVS library.
///
/// It encapsulates all other error types.
#[derive(Debug)]
pub enum KVSError {
    /// Represents an I/O error.
    FailedIO(IOError),
    /// Represents an error during data conversion.
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

/// Represents an I/O error that can occur in the KVS library.
#[derive(Debug, PartialEq)]
pub enum IOError {
    /// Failed to write bytes to a file.
    FailedWriteBytes(String),
    /// Failed to open a file.
    FailedOpenFile(PathBuf, String),
    /// Failed to create a file.
    FailedCreateFile(PathBuf, String),
    /// Failed to truncate the Write-Ahead Log.
    FailedTruncateWAL(String),
    /// Failed to read from a file.
    FailedReadFile(String),
    /// Failed to remove a file.
    FailedRemoveFile(PathBuf, String),
    /// Failed to get the size of a file.
    FailedGetFileSize(PathBuf, String),
    /// Failed to get a file path from a directory.
    FailedGetFilePath(PathBuf, String),
    /// Failed to seek within a file.
    FailedSeek(String),
    /// The specified directory was not found.
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

/// Represents an error that can occur during data conversion.
#[derive(Debug)]
pub enum ConvertError {
    /// Failed to convert bytes to a `Value`.
    FailedBytesToValue(String),
    /// Failed to convert bytes to a `String`.
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
