use std::{
    fs::File,
    io::{BufWriter, Write},
};
use crate::{
    error::IOError,
    value::Value
};

pub fn write_key_value(buf_writer: &mut BufWriter<File>, key: &str, value: &Value) -> Result<usize, IOError>{
    let key_bytes: Vec<u8> = [&key.len().to_be_bytes(), key.as_bytes()].concat();
    let value_bytes: Vec<u8> = value.clone().to_bytes();
    let bytes: Vec<u8> = [key_bytes, value_bytes, vec![value.is_delete as u8]].concat();

    match buf_writer.write(&bytes) {
        Ok(u) => Ok(u),
        Err(e) => Err(IOError::FailedWriteBytes(e.to_string()))
    }
}

pub fn write_index(buf_writer: &mut BufWriter<File>, key: &str, pointer: usize) -> Result<usize, IOError> {
    let key_bytes: Vec<u8> = [&key.len().to_be_bytes(), key.as_bytes()].concat();
    let pointer_bytes: Vec<u8> = pointer.to_be_bytes().to_vec();

    match buf_writer.write(&[key_bytes, pointer_bytes].concat()) {
        Ok(u) => Ok(u),
        Err(e) => Err(IOError::FailedWriteBytes(e.to_string()))
    }
}