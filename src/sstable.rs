use std::{collections::{BTreeMap, HashMap}, error::Error, fs::File, io::BufWriter, path::PathBuf};

use crate::{error::SSTableError, value::Value, file_io::{write_index, write_key_value}};

#[derive(Debug)]
pub struct SSTable {
    data_path: PathBuf,
    index_path: PathBuf,
    index: HashMap<String, usize>
}

impl SSTable {
    pub fn create(data_dir: &PathBuf, memtable: &BTreeMap<String, Value>) -> Result<Self, SSTableError> {
        let filename = "sstab";

        let mut data_path = data_dir.clone();
        data_path.push(format!("{}.dat", filename));
        let mut data_writer: BufWriter<File> = match get_bufreader(&data_path) {
            Ok(bw) => bw,
            Err(e) => return Err(e)
        };


        let mut index_path = data_dir.clone();
        index_path.push(format!("{}.idx", filename));
        let mut index_writer = match get_bufreader(&index_path) {
            Ok(bw) => bw,
            Err(e) => return Err(e)
        };

        let mut index = HashMap::new();
        let mut pointer: usize = 0;
        for (k, v) in memtable.iter() {
            match write_index(&mut index_writer, k, pointer) {
                Ok(_) => {},
                Err(e) => unimplemented!()
            }
            index.insert(k.to_string(), pointer);
            pointer += match write_key_value(&mut data_writer, k, v) {
                Ok(size) => size,
                Err(e) => unimplemented!()
            };
        }

        Ok(SSTable { 
            data_path: data_path,
            index_path: index_path,
            index: index
        })
    }
}

fn get_bufreader(path: &PathBuf) -> Result<BufWriter<File>, SSTableError> {
    match File::create(path) {
        Ok(f) => Ok(BufWriter::new(f)),
        Err(e) => unimplemented!()
    }
}