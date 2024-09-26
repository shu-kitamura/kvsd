mod record;
mod error;

use record::Record;
// use chrono::Local;

fn main() {
    let r = Record::new("k", "v", 0, true);
    r.to_vec();
    let v: Vec<u8> = vec![
        0, 0, 0, 0, 0, 0, 0, 3,  // 3 (length of key)
        102, 97, 110,            // fat
        0, 0, 0, 0, 0, 0, 0, 5,  // 5 (length of value)
        118, 97, 108, 117, 101,  // value
        0, 0, 0, 0, 0, 0, 0, 0,  // timestamp
        1                        // true
    ];
    println!("{:?}", Record::from_vec(v));
}
