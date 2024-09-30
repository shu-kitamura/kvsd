use std::{collections::{BTreeMap, HashMap}, fs::File, io::{BufReader, BufWriter}, path::PathBuf};

use crate::{error::{IOError, SSTableError}, file_io::{read_key_value, write_index, write_key_value}, value::Value};

#[derive(Debug)]
pub struct SSTable {
    data_path: PathBuf,
    index_path: PathBuf,
    index: HashMap<String, usize>
}

impl SSTable {
    pub fn create(data_dir: &PathBuf, memtable: &BTreeMap<String, Value>, filename: &str) -> Result<Self, IOError> {
        let mut data_path: PathBuf = data_dir.clone();
        data_path.push(format!("{}.dat", filename));
        let mut data_writer: BufWriter<File> = match get_bufwriter(&data_path) {
            Ok(bw) => bw,
            Err(e) => return Err(e)
        };


        let mut index_path: PathBuf = data_dir.clone();
        index_path.push(format!("{}.idx", filename));
        let mut index_writer = match get_bufwriter(&index_path) {
            Ok(bw) => bw,
            Err(e) => return Err(e)
        };

        let mut index: HashMap<String, usize> = HashMap::new();
        let mut pointer: usize = 0;
        for (k, v) in memtable.iter() {
            if let Err(e) = write_index(&mut index_writer, k, pointer) {
                return Err(e)
            }
            index.insert(k.to_string(), pointer);
            pointer += match write_key_value(&mut data_writer, k, v) {
                Ok(size) => size,
                Err(e) => return Err(e)
            };
        }

        Ok(SSTable { 
            data_path: data_path,
            index_path: index_path,
            index: index
        })
    }

    pub fn get(&self, key: &str) -> Result<Option<Value>, IOError> {
        let pointer = match self.index.get(key) {
            Some(p) => *p,
            None => return Ok(None)
        };

        let file = match File::open(self.data_path.clone()) {
            Ok(f) => f,
            Err(e) => return Err(IOError::FailedOpenFile(self.data_path.clone(), e.to_string()))
        };
        let mut buf_reader: BufReader<File> = BufReader::new(file);

        let (_, bytes) = match read_key_value(&mut buf_reader, pointer) {
            Ok(kv) => kv,
            Err(e) => return Err(e)
        };

        let value = match Value::from_bytes(bytes) {
            Ok(v) => v,
            Err(e) => unimplemented!()
        };

        Ok(Some(value))
    }
}

fn get_bufwriter(path: &PathBuf) -> Result<BufWriter<File>, IOError> {
    match File::create(path) {
        Ok(f) => Ok(BufWriter::new(f)),
        Err(e) => return Err(
            IOError::FailedCreateFile(path.clone(), e.to_string())
        )
    }
}