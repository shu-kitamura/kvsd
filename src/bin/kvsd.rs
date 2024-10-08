use std::{io::{Read, Write}, net::{TcpListener, TcpStream}};

use kvsd::KVS;


const DEFAULT_PORT: &str = "54321";
const DEFAULT_HOST: &str = "localhost";

fn main() {
    let mut kvs: KVS = match KVS::new() {
        Ok(k) => k,
        Err(e) => {
            eprintln!("{} [ERROR] {}", get_now(), e);
            return
        }
    };

    let mut previous_compaction: i64 = chrono::Local::now().timestamp();

    let address: String = format!("{DEFAULT_HOST}:{DEFAULT_PORT}");

    let listner: TcpListener = match TcpListener::bind(&address) {
        Ok(tl) => tl,
        Err(e) => {
            eprintln!(
                "{} [ERROR] Failed to bind IP address '{}', because the following error is occured.\n{}",
                get_now(),
                address,
                e
            );
            return
        }
    };

    for stream_result in listner.incoming() {
        match stream_result {
            Ok(stream) => {
                handle(&stream, &mut kvs);
                let now_timestamp: i64 = chrono::Local::now().timestamp();
                if now_timestamp - previous_compaction > 86_400_000 {
                    if let Err(e) = kvs.compaction() {
                        eprintln!("{} [ERROR] Failed compaction.\n{}", get_now(), e);
                        previous_compaction = now_timestamp;
                    }
                }
            },
            Err(e) => {
                eprintln!("{} [ERROR] {}", get_now(), e)
            }
        }
    }
}


fn handle(mut stream: &TcpStream, kvs: &mut KVS) {
    let mut buf: [u8; 1024] = [0u8; 1024];
    stream.read(&mut buf).unwrap();

    let input: String  = match String::from_utf8(buf.to_vec()) {
        Ok(string) => string.replace("\0", "").trim().to_string(),
        Err(e) => {
            eprintln!("{} [ERROR] {}", get_now(), e);
            return
        }
    };

    let cmd: Vec<&str> = input.split_whitespace().collect();
    println!("{} [INFO] Recieved request {:?}", get_now(), cmd);

    match cmd[0] {
        "get" => {
            let option_value = kvs.get(cmd[1]).unwrap();
            if let Some(value) = option_value {
                let string_value: String = format!("{value}");
                let bytes: &[u8] = string_value.as_bytes();
                if let Err(e) = stream.write(bytes) {
                    eprintln!("{} [ERROR] {}", get_now(), e)
                };
            }
        },
        "delete" => {
            if let Err(e) = kvs.delete(cmd[1]) {
                eprintln!("{} [ERROR] {}", get_now(), e)
            };
        },
        "put" => {
            if let Err(e) = kvs.put(cmd[1], cmd[2]) {
                eprintln!("{} [ERROR] {}", get_now(), e)
            };
        }
        _ => unreachable!()
    }
}

fn get_now() -> String {
    let now = chrono::Local::now();
    now.format("%F %T%.3f").to_string()
}