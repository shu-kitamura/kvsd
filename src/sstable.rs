use std::path::PathBuf;

use crate::{error::SSTableError, record::Record};

pub struct SSTable {
    data_path: PathBuf,
    index_path: PathBuf
}

impl SSTable {
    pub fn new(bytes: Vec<Record>, data_path: &str, index_path: &str) -> Result<Self, SSTableError> {
        let dp: PathBuf = PathBuf::from(data_path);
        let ip: PathBuf = PathBuf::from(index_path);

        if dp.exists() || ip.exists() {
            return Err(SSTableError::FailedCreate(
                format!("{} or {} is already exist.",
                dp.to_str().unwrap(),
                ip.to_str().unwrap())
            ))
        }

        Ok(SSTable { 
            data_path: dp,
            index_path: ip
        })
    }
}