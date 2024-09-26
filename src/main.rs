use kvsd::KVS;

mod error;
mod kvsd;
mod sstable;
mod value;

fn main() {
    let mut kvs = KVS::new();
    kvs.put("key", "value");
    kvs.delete("key");

    kvs.put("xxx", "xxxxx");
    println!("{:?}", kvs.get("xxx"));
    println!("{:?}", kvs.get("key"));

    println!("{:?}", kvs.memtable)
}
