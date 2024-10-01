use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Seek, Write},
    path::PathBuf
};
use crate::{
    error::IOError,
    value::Value
};

pub fn write_key_value(buf_writer: &mut BufWriter<File>, key: &str, value: &Value) -> Result<usize, IOError>{
    let key_bytes: Vec<u8> = [&key.len().to_be_bytes(), key.as_bytes()].concat();
    let value_bytes: Vec<u8> = value.clone().to_bytes();
    let bytes: Vec<u8> = [key_bytes, value_bytes, vec![value.is_deleted() as u8]].concat();

    match buf_writer.write(&bytes) {
        Ok(u) => Ok(u),
        Err(e) => Err(IOError::FailedWriteBytes(e.to_string()))
    }
}

fn read(buf_reader: &mut BufReader<File>, length: usize) -> Result<Vec<u8>, IOError> {
    let mut bytes: Vec<u8> = vec![0; length];
    match buf_reader.read(&mut bytes) {
        Ok(_) => Ok(bytes),
        Err(e) => Err(IOError::FailedReadFile(e.to_string())) // read に失敗したエラーを出したい IOError
    }
}

fn read_length(buf_reader: &mut BufReader<File>) -> Result<usize, IOError> {
    let mut bytes:[u8; 8]  = [0; 8];
    match buf_reader.read(&mut bytes) {
        Ok(_) => Ok(usize::from_be_bytes(bytes)),
        Err(e) => Err(IOError::FailedReadFile(e.to_string()))
    }
}

pub fn read_key_value(buf_reader: &mut BufReader<File>, offset: usize) -> Result<(Vec<u8>, Vec<u8>), IOError> {
    if let Err(e) = buf_reader.seek(std::io::SeekFrom::Start(offset as u64)) {
        return Err(IOError::FailedSeek(e.to_string()))
    };

    let mut map: HashMap<&str, Vec<u8>> = HashMap::new();

    for k in ["key", "value"] {
        let length = read_length(buf_reader)?;

        match read(buf_reader, length) {
            Ok(v) => map.insert(k, v),
            Err(e) => return Err(e)
        };
    }

    let key = map.get("key").map(|v| v.clone()).unwrap();
    let value = map.get("value").map(|v| v.clone()).unwrap();

    Ok((key, value))
}

pub fn get_filesize(path: &PathBuf) -> Result<usize, IOError> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len() as usize),
        Err(e) => return Err(IOError::FailedGetFileSize(path.clone(), e.to_string()))
    }
}