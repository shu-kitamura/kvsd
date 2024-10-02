
use std::{
    io::{self, Write},
    usize
};

use error::ParseError;
use kvsd::KVS;

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

    let mut compaction_timestamp: i64 = chrono::Local::now().timestamp();

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
            "put" => {
                let (key, value) = (&args[0], &args[1]);
                if let Err(e) = kvs.put(key, value) {
                    eprintln!("{}", e);
                }
            },
            "delete" => {
                let key = &args[0];
                if let Err(e) = kvs.delete(key) {
                    eprintln!("{}", e);
                }
            },
            "get" => {
                let key = &args[0];
                match kvs.get(key) {
                    Ok(opt) => if let Some(value) = opt {
                        println!("{value}");
                    }
                    Err(e) =>eprintln!("{}", e)
                }
            },
            "exit" => break,
            _ => unreachable!()
        };

        let now = chrono::Local::now().timestamp();
        if now - compaction_timestamp > 36000000 {
            if let Err(e) = kvs.compaction() {
                eprintln!("{}", e);
            };
            compaction_timestamp = now;
        }
    }
}

fn parse_input(input: String) -> Result<(Option<String>, Vec<String>), ParseError> {
    let input_vec: Vec<&str> = input.split_whitespace().collect();
    match input_vec.len() {
        0 => Ok((None, Vec::new())),
        1 => {
            let operation: String = input_vec[0].to_string();
            if check_args(&operation, 0)? {
                Ok((Some(operation), Vec::new()))                
            } else {
                Err(ParseError::InvalidArguments)
            }
        }
        2 | 3 => {
            let operation: String = input_vec[0].to_string();
            let args: Vec<String> = input_vec[1..].iter().map(|s| s.to_string()).collect();
            if check_args(&operation, args.len())? {
                Ok((Some(operation), args))
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

// ----- test -----

mod tests {
    use crate::{
        check_args,
        error::ParseError,
        parse_input
    };

    #[test]
    fn test_parse_input() {
        let empty_vec: Vec<String> = Vec::new();

        // 空文字を受け取るケース
        let input1 = String::new();
        let (cmd1, args1) = parse_input(input1).unwrap();
        assert_eq!(cmd1, None);
        assert_eq!(args1, empty_vec);

        // コマンドのみを受け取るケース
        let input2 = String::from("exit");
        let (cmd2, args2) = parse_input(input2).unwrap();
        assert_eq!(cmd2, Some("exit".to_string()));
        assert_eq!(args2, empty_vec);

        // コマンドと引数1つを受け取るケース
        let input3 = String::from("get k1");
        let (cmd3, args3) = parse_input(input3).unwrap();
        assert_eq!(cmd3, Some("get".to_string()));
        assert_eq!(args3, vec!["k1".to_string()]);

        // コマンドと引数2つを受け取るケース
        let input4 = String::from("put k1 value1");
        let (cmd4, args4) = parse_input(input4).unwrap();
        assert_eq!(cmd4, Some("put".to_string()));
        assert_eq!(args4, vec!["k1".to_string(), "value1".to_string()]);

        // コマンドと引数3つを受け取るケース(エラー)
        let input5 = String::from("put k1 value1 error");
        let err = parse_input(input5).unwrap_err();
        assert_eq!(err, ParseError::InvalidArguments);
    }

    #[test]
    fn test_check_args() {
        // put のケース
        assert_eq!(check_args("put", 1), Ok(false));
        assert_eq!(check_args("put", 2), Ok(true));
        assert_eq!(check_args("put", 3), Ok(false));

        // get, delete のケース
        assert_eq!(check_args("get", 1), Ok(true));
        assert_eq!(check_args("get", 2), Ok(false));
        assert_eq!(check_args("delete", 1), Ok(true));
        assert_eq!(check_args("delete", 2), Ok(false));

        // exit のケース
        assert_eq!(check_args("exit", 0), Ok(true));
        assert_eq!(check_args("exit", 1), Ok(false));

        // 他のコマンドのケース(エラー)
        assert_eq!(check_args("error", 0), Err(ParseError::CommandNotDefine("error".to_string())))
    }
}