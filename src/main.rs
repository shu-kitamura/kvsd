use std::{fs::{self, File}, io::BufReader, path::PathBuf};

use chrono::TimeZone;
use kvsd::KVS;
use sstable::SSTable;
use file_io::read_key_value;
use value::Value;

mod error;
mod kvsd;
mod sstable;
mod value;
mod wal;
mod file_io;

fn main() {
    let mut kvs = KVS::new();
    kvs.put("k1", "value1");
    kvs.put("k3", "value3");
    kvs.put("k2", "value2");
    kvs.put("k6", "value6");
    kvs.put("k8", "value8");
    kvs.put("k4", "value4");
    kvs.put("k0", "value0");
    kvs.put("k5", "value5");
    kvs.put("k9", "value9");
    kvs.put("k7", "value7");

    kvs.wal.recovery();
    println!("{:?}", kvs.memtable);
    // kvs.flush();
    // // println!("{:?}", kvs.get("k5"));
    // // println!("{:?}", kvs.get("k1"));
    // // println!("{:?}", kvs.get("k999"));]

    // let data_dir: PathBuf = PathBuf::from("./data/");
    // let files = fs::read_dir(data_dir).unwrap();
    // for file in files {
    //     if let Ok(f) = file {
    //         println!("{:?}", f.path())
    //     }
    // }

    let jp = Value::new("Japan", false);
    let us = Value::new("USA", false);

    println!("{jp}");
    println!("{us}");
}

fn row_len(key: String, value: crate::value::Value) -> usize {
    key.len() + value.len() + 8
}
