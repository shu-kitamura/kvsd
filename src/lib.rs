mod error;
mod file_io;
mod sstable;
mod value;
mod wal;

use std::{collections::BTreeMap, fs, path::PathBuf};

use error::{IOError, KVSError};
use sstable::SSTable;
use value::Value;
use wal::WriteAheadLog;

/// A key-value store.
pub struct KVS {
    /// In-memory key-value store.
    memtable: BTreeMap<String, Value>,
    /// The maximum number of key-value pairs to store in the memtable.
    limit: usize,
    /// The directory where the data files are stored.
    data_dir: PathBuf,
    /// The write-ahead log for durability.
    wal: WriteAheadLog,
    /// The list of SSTables.
    sstables: Vec<SSTable>,
}

const DEFAULT_DATA_DIR: &str = "./data/";
const DEFAULT_WAL_FILENAME: &str = "wal";

impl KVS {
    /// Creates a new `KVS` instance.
    ///
    /// This function initializes the `KVS` by:
    /// - Setting the data directory.
    /// - Loading existing SSTables.
    /// - Initializing the write-ahead log.
    /// - Recovering the memtable from the WAL.
    pub fn new() -> Result<Self, KVSError> {
        let data_dir: PathBuf = PathBuf::from(DEFAULT_DATA_DIR);
        if !data_dir.is_dir() {
            return Err(KVSError::FailedIO(IOError::DirectoryNotFound(data_dir)));
        }

        let sstables: Vec<SSTable> = get_sstables(&data_dir)?;
        let mut wal: WriteAheadLog = WriteAheadLog::new(&data_dir, DEFAULT_WAL_FILENAME)?;
        let memtable: BTreeMap<String, Value> = wal.recovery()?;

        Ok(KVS {
            memtable,
            limit: 1024,
            wal,
            data_dir,
            sstables,
        })
    }

    /// Inserts a key-value pair into the store.
    ///
    /// # Arguments
    ///
    /// * `k` - The key.
    /// * `v` - The value.
    pub fn put(&mut self, k: &str, v: &str) -> Result<(), IOError> {
        let value: Value = Value::new(v, false);
        self.put_key_value(k, value)
    }

    /// Deletes a key-value pair from the store.
    ///
    /// # Arguments
    ///
    /// * `k` - The key to delete.
    pub fn delete(&mut self, k: &str) -> Result<(), IOError> {
        let value: Value = Value::new("", true);
        self.put_key_value(k, value)
    }

    /// A helper function to put a key-value pair into the memtable and WAL.
    fn put_key_value(&mut self, key: &str, value: Value) -> Result<(), IOError> {
        self.wal.write(key, &value)?;
        self.memtable.insert(key.to_string(), value);

        if self.limit < self.memtable.len() {
            self.flush()?;
        }

        Ok(())
    }

    /// Retrieves a value from the store by its key.
    ///
    /// It first searches the memtable, then the SSTables.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to retrieve.
    pub fn get(&mut self, key: &str) -> Result<Option<Value>, KVSError> {
        if let Some(value) = self.memtable.get(key) {
            return match value.is_deleted() {
                true => Ok(None),
                false => Ok(Some(value.clone())),
            };
        }

        if let Some(value) = self.get_from_sstable(key)? {
            return match value.is_deleted() {
                true => Ok(None),
                false => Ok(Some(value)),
            };
        }
        Ok(None)
    }

    /// Retrieves a value from the SSTables by its key.
    fn get_from_sstable(&mut self, key: &str) -> Result<Option<Value>, KVSError> {
        for sstable in self.sstables.iter().rev() {
            if let Some(value) = sstable.get(key)? {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    /// Flushes the memtable to an SSTable.
    pub fn flush(&mut self) -> Result<(), IOError> {
        let timestamp = chrono::Local::now().timestamp();
        match SSTable::create(&self.data_dir, &self.memtable, &timestamp.to_string()) {
            Ok(sst) => self.sstables.push(sst),
            Err(e) => return Err(e),
        };

        match self.wal.clear() {
            Ok(_) => self.memtable.clear(),
            Err(e) => return Err(e),
        };

        Ok(())
    }

    /// Compacts the SSTables into a single SSTable.
    pub fn compaction(&mut self) -> Result<(), KVSError> {
        let mut btm: BTreeMap<String, Value> = BTreeMap::new();
        for sstable in self.sstables.iter() {
            for key in sstable.keys() {
                if let Some(value) = sstable.get(key)? {
                    btm.insert(key.to_string(), value);
                }
            }
        }

        clear_sstables(&self.data_dir)?;
        self.sstables.clear();

        let timestamp = chrono::Local::now().timestamp();
        let sstable: SSTable = SSTable::create(&self.data_dir, &btm, &timestamp.to_string())?;
        self.sstables.push(sstable);

        Ok(())
    }
}

/// Gets a list of SSTables from the data directory.
fn get_sstables(data_dir: &PathBuf) -> Result<Vec<SSTable>, KVSError> {
    let data_files: Vec<PathBuf> = get_data_files(data_dir)?;
    let mut sstables: Vec<SSTable> = Vec::new();

    for file in data_files {
        let sstable = SSTable::from_file(file)?;
        sstables.push(sstable)
    }
    Ok(sstables)
}

/// Gets a list of data files from the data directory.
fn get_data_files(data_dir: &PathBuf) -> Result<Vec<PathBuf>, IOError> {
    let files: fs::ReadDir = match fs::read_dir(data_dir) {
        Ok(read_dir) => read_dir,
        Err(e) => return Err(IOError::FailedGetFilePath(data_dir.clone(), e.to_string())),
    };

    let mut data_files: Vec<PathBuf> = Vec::new();

    for result in files {
        let data_file: PathBuf = match result {
            Ok(dir_entry) => dir_entry.path(),
            Err(e) => return Err(IOError::FailedGetFilePath(data_dir.clone(), e.to_string())),
        };

        let mut extention: &str = "";
        if let Some(ext_os_str) = data_file.extension() {
            if let Some(ext_str) = ext_os_str.to_str() {
                extention = ext_str
            }
        };

        if extention == "dat" {
            data_files.push(data_file)
        };
    }
    Ok(data_files)
}

/// Clears all SSTables from the data directory.
fn clear_sstables(data_dir: &PathBuf) -> Result<(), IOError> {
    let data_files: Vec<PathBuf> = get_data_files(data_dir)?;

    for file in data_files {
        if let Err(e) = fs::remove_file(&file) {
            return Err(IOError::FailedRemoveFile(file, e.to_string()));
        }
    }

    Ok(())
}
