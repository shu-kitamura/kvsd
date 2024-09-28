use std::{fs::File, io::BufReader, path::PathBuf};

use kvsd::KVS;
use sstable::SSTable;
use file_io::read_key_value;

mod error;
mod kvsd;
mod sstable;
mod value;
mod wal;
mod file_io;

fn main() {
    // let mut kvs = KVS::new();
    // kvs.put("k1", "value");
    // kvs.put("k3", "value");
    // kvs.put("k2", "value");
    // kvs.put("k6", "value");
    // kvs.put("k8", "value");
    // kvs.put("k4", "value");
    // kvs.put("k0", "value");
    // kvs.put("k5", "value");
    // kvs.put("k9", "value");
    // kvs.put("k7", "value");

    // kvs.flush();
    // println!("{:?}", kvs.memtable);
    let file: File = File::open("data/sstab.dat").unwrap();
    let mut br: BufReader<File> = BufReader::new(file);

    let (k, v) = read_key_value(&mut br, 0);
    println!("{k}, {:?}", v);
    println!("{}", row_len(k, v));
}

fn row_len(key: String, value: crate::value::Value) -> usize {
    key.len() + value.len() + 8
}
