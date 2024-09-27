use kvsd::KVS;

mod error;
mod kvsd;
mod sstable;
mod value;

fn main() {
    let mut kvs = KVS::new();
    kvs.put("k1", "value");
    kvs.put("k3", "value");
    kvs.put("k2", "value");
    kvs.put("k6", "value");
    kvs.put("k8", "value");
    kvs.put("k4", "value");
    kvs.put("k0", "value");
    kvs.put("k5", "value");
    kvs.put("k9", "value");
    kvs.put("k7", "value");

    kvs.flush();
    println!("{:?}", kvs.memtable);
}
