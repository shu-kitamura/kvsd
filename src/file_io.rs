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

fn read(buf_reader: &mut BufReader<File>, offset: usize) -> (usize, String) {
    if let Err(e) = buf_reader.seek(std::io::SeekFrom::Start(offset as u64)) {
        unimplemented!()
    };

    let mut read_size = 0;

    let mut len_bytes:[u8; 8]  = [0; 8];
    let size = match buf_reader.read(&mut len_bytes) {
        Ok(n) => {
            read_size += n;
            usize::from_be_bytes(len_bytes)
        }
        Err(e) => unimplemented!()
    };

    let mut str_bytes: Vec<u8> = vec![0; size];
    match buf_reader.read(&mut str_bytes) {
        Ok(n) => {
            read_size += n;
            match String::from_utf8(str_bytes) {
                Ok(s) => (read_size, s),
                Err(e) => unimplemented!() // String への 変換に失敗したエラーを出したい IOError
            }
        }
        Err(e) => unimplemented!() // read に失敗したエラーを出したい IOError
    }
}

pub fn read_key_value(buf_reader: &mut BufReader<File>, mut offset: usize) -> (String, Value) {
    let (n, key) = read(buf_reader, offset);
    offset += n;

    let (n, v) = read(buf_reader, offset);

    let mut bytes: [u8; 1] = [99;1];
    let is_delete = match buf_reader.read(&mut bytes) {
        Ok(a) => match bytes[0] {
            0 => false,
            1 => true,
            _ => unimplemented!()
        },
        Err(e) => unimplemented!() 
    };

    let value: Value = Value::new(&v, is_delete);
    (key, value)
}