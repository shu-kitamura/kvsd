use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Seek, Write},
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

pub fn write_index(buf_writer: &mut BufWriter<File>, key: &str, pointer: usize) -> Result<usize, IOError> {
    let key_bytes: Vec<u8> = [&key.len().to_be_bytes(), key.as_bytes()].concat();
    let pointer_bytes: Vec<u8> = pointer.to_be_bytes().to_vec();

    match buf_writer.write(&[key_bytes, pointer_bytes].concat()) {
        Ok(u) => Ok(u),
        Err(e) => Err(IOError::FailedWriteBytes(e.to_string()))
    }
}

fn read(buf_reader: &mut BufReader<File>, length: usize) -> Vec<u8> {
    let mut bytes: Vec<u8> = vec![0; length];
    match buf_reader.read(&mut bytes) {
        Ok(_) => bytes,
        Err(e) => unimplemented!() // read に失敗したエラーを出したい IOError
    }
}

fn read_length(buf_reader: &mut BufReader<File>) -> usize {
    let mut len_bytes:[u8; 8]  = [0; 8];
    match buf_reader.read(&mut len_bytes) {
        Ok(_) => usize::from_be_bytes(len_bytes),
        Err(e) => unimplemented!()
    }
}

fn read_key(buf_reader: &mut BufReader<File>) -> String {
    let length: usize = read_length(buf_reader);
    let bytes: Vec<u8> = read(buf_reader, length);
    match String::from_utf8(bytes) {
        Ok(key) => key,
        Err(e) => unimplemented!()
    }
}

fn read_value(buf_reader: &mut BufReader<File>) -> Value {
    let length: usize = read_length(buf_reader);
    let bytes: Vec<u8> = read(buf_reader, length);
    match Value::from_bytes(bytes) {
        Ok(value) => value,
        Err(e) => unimplemented!()
    }
}

pub fn read_key_value(buf_reader: &mut BufReader<File>, offset: usize) -> (String, Value) {
    if let Err(e) = buf_reader.seek(std::io::SeekFrom::Start(offset as u64)) {
        unimplemented!()
    };

    let key: String = read_key(buf_reader);
    let value: Value = read_value(buf_reader);    

    (key, value)
}

pub fn read_index(buf_reader: &mut BufReader<File>, offset: usize) -> (String, usize) {
    if let Err(e) = buf_reader.seek(std::io::SeekFrom::Start(offset as u64)) {
        unimplemented!()
    };

    let key: String = read_key(buf_reader);

    let mut bytes: [u8; 8] = [0; 8];
    let pointer = match buf_reader.read(&mut bytes) {
        Ok(_) => usize::from_be_bytes(bytes),
        Err(e) => unimplemented!()
    };

    (key, pointer)
}