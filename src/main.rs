
use std::{collections::HashMap, env::args, hash::Hash, io::{self, Write}, path::PathBuf, usize};

use error::{ConvertError, ParseError};
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
    let mut kvs: KVS = match KVS::new() {
        Ok(kvs) => kvs,
        Err(e) => {
            eprintln!("{}", e);
            return
        }
    };

    loop {
        print!("> ");

        match io::stdout().flush() {
            Ok(()) => {},
            Err(e) => {
                eprintln!("{}", e);
                return
            }
        }

        let mut input: String = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(_)=> input.trim(),
            Err(e) => {
                eprintln!("{}", e);
                return
            }
        };

        let (oper, args) = match parse_input(input) {
            Ok(parsed) => parsed,
            Err(e) => {
                eprintln!("{}", e);
                continue;
            }
        };

        let operation: String = match oper {
            Some(s) => s,
            None => continue
        };

        match operation.as_str() {
            "put" => println!("put execute"),
            "delete" => println!("delete execute"),
            "get" => println!("get execute"),
            "exit" => break,
            _ => unreachable!()
        };
    }
    // println!("{:?}", kvs.get("k1"));
    // // kvs.put("k3", "value3_20241002");
    // // kvs.put("k2", "value2_20241002");
    // // kvs.put("k6", "value6_20241002");
    // // kvs.put("k8", "value8_20241002");
    // // kvs.put("k4", "value4_20241002");
    // // kvs.put("k0", "value0_20241002");
    // kvs.put("k5", "value5_20241002");
    // // kvs.put("k9", "value9_20241002");
    // // kvs.put("k7", "value7_20241002");
    // kvs.delete("k5");

    // kvs.flush();
    // println!("{:?}", kvs.get("k5"));
    // // println!("{:?}", kvs.get("k1"));
    // // println!("{:?}", kvs.get("k999"));

    // // let path = PathBuf::from("./data/1727768621.dat");
    // // let sstab = SSTable::from_file(path).unwrap();
    // // sstab.keys();
}

fn parse_input(input: String) -> Result<(Option<String>, Option<Vec<String>>), ParseError> {
    let input_vec: Vec<&str> = input.split_whitespace().collect();
    match input_vec.len() {
        0 => Ok((None, None)),
        1 => {
            let operation: String = input_vec[0].to_string();
            if check_args(&operation, 0)? {
                Ok((Some(operation), None))                
            } else {
                Err(ParseError::InvalidArguments)
            }
        }
        2 | 3 => {
            let operation: String = input_vec[0].to_string();
            let args: Vec<String> = input_vec[1..].iter().map(|s| s.to_string()).collect();
            if check_args(&operation, args.len())? {
                Ok((Some(operation), Some(args)))
            } else {
                Err(ParseError::InvalidArguments)
            }
        }
        _ => {
            let operation: String = input_vec[0].to_string();
            if let Err(e) = check_args(&operation, usize::MAX) {
                Err(e)
            } else {
                Err(ParseError::InvalidArguments)
            }
        }
    }
}

fn check_args(operation: &str, args_len: usize) -> Result<bool, ParseError> {
    let check_res: bool = match operation {
        "put" => args_len == 2,
        "get" | "delete" => args_len == 1,
        "exit" => args_len == 0,
        _ => return Err(ParseError::CommandNotDefine(operation.to_string()))
    };
    Ok(check_res)
}