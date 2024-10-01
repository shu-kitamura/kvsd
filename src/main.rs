
use error::ConvertError;
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
    let mut kvs = KVS::new().unwrap();
    kvs.put("k1", "value1_20241001");
    kvs.put("k3", "value3_20241001");
    kvs.put("k2", "value2_20241001");
    kvs.put("k6", "value6_20241001");
    kvs.put("k8", "value8_20241001");
    kvs.put("k4", "value4_20241001");
    kvs.put("k0", "value0_20241001");
    kvs.put("k5", "value5_20241001");
    kvs.put("k9", "value9_20241001");
    kvs.put("k7", "value7_20241001");
    kvs.delete("k5");

    kvs.flush();
    println!("{:?}", kvs.get("k5"));
    // println!("{:?}", kvs.get("k1"));
    // println!("{:?}", kvs.get("k999"));
}