use std::{
    fmt::{self, Display},
    io::{self, Read, Write},
    net::TcpStream,
    usize,
};

/// Represents an error that can occur when parsing a command.
#[derive(Debug, PartialEq)]
pub enum CommandError {
    /// The specified command is not defined.
    CommandNotDefine(String),
}

impl Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommandError::CommandNotDefine(cmd) => {
                write!(f, "ParseError: The command '{cmd}' is not defined.")
            }
        }
    }
}

const DEFAULT_PORT: usize = 54321;
const DEFAULT_HOST: &str = "localhost";

/// The main function for the key-value store shell.
fn main() {
    loop {
        print!("> ");

        match io::stdout().flush() {
            Ok(()) => {}
            Err(e) => {
                eprintln!("{e}");
                return;
            }
        }

        let mut input: String = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(_) => input.trim(),
            Err(e) => {
                eprintln!("{e}");
                return;
            }
        };

        let (oper, check_res) = match check_input(&input) {
            Ok(tuple) => tuple,
            Err(e) => {
                eprintln!("{e}");
                continue;
            }
        };

        match check_res {
            Some(b) => {
                if b {
                    match oper.as_str() {
                        "exit" => return,
                        "put" | "delete" => {
                            if let Err(e) = send_request(DEFAULT_HOST, DEFAULT_PORT, &input) {
                                eprintln!("{e}")
                            }
                        }
                        "get" => match send_request(DEFAULT_HOST, DEFAULT_PORT, &input) {
                            Ok(res) => match String::from_utf8(res.to_vec()) {
                                Ok(value) => {
                                    println!("{}", value.replace("\0", "").trim());
                                }
                                Err(e) => eprintln!("{e}"),
                            },
                            Err(e) => eprintln!("{e}"),
                        },
                        _ => unreachable!(),
                    }
                } else {
                    eprintln!("Invalid arguments.")
                }
            }
            None => continue,
        }
    }
}

/// Sends a request to the key-value store server.
fn send_request(host: &str, port: usize, request: &str) -> Result<[u8; 1024], io::Error> {
    let address: String = format!("{host}:{port}");
    let mut stream: TcpStream = TcpStream::connect(address)?;

    stream.write(request.as_bytes())?;

    // response を read する
    let mut response: [u8; 1024] = [0u8; 1024];
    stream.read(&mut response)?;

    Ok(response)
}

/// Checks the validity of the user's input.
fn check_input(input: &str) -> Result<(String, Option<bool>), CommandError> {
    let input_vec: Vec<&str> = input.split_whitespace().collect();
    match input_vec.len() {
        0 => Ok((String::new(), None)),
        1 => {
            let operation: String = input_vec[0].to_string();
            let check_result: bool = check_args(&operation, 0)?;
            Ok((operation, Some(check_result)))
        }
        2 | 3 => {
            let operation: String = input_vec[0].to_string();
            let args: Vec<String> = input_vec[1..].iter().map(|s| s.to_string()).collect();
            let check_result: bool = check_args(&operation, args.len())?;
            Ok((operation, Some(check_result)))
        }
        _ => {
            let operation: String = input_vec[0].to_string();
            let check_result: bool = check_args(&operation, usize::MAX)?;
            Ok((operation, Some(check_result)))
        }
    }
}

/// Checks if the number of arguments for a given operation is correct.
fn check_args(operation: &str, args_len: usize) -> Result<bool, CommandError> {
    let check_res: bool = match operation {
        "put" => args_len == 2,
        "get" | "delete" => args_len == 1,
        "exit" => args_len == 0,
        _ => return Err(CommandError::CommandNotDefine(operation.to_string())),
    };
    Ok(check_res)
}

// ----- test -----

mod tests {

    #[test]
    fn test_check_input() {
        // 空文字を受け取るケース
        let input1: String = String::new();
        let (opr, check_res) = crate::check_input(&input1).unwrap();
        assert_eq!(opr, String::from(""));
        assert_eq!(check_res, None);

        // コマンドのみを受け取るケース
        let input2: String = String::from("exit");
        let (opr, check_res) = crate::check_input(&input2).unwrap();
        assert_eq!(opr, String::from("exit"));
        assert_eq!(check_res, Some(true));

        // コマンドと引数1つを受け取るケース
        let input3 = String::from("get k1");
        let (opr, check_res) = crate::check_input(&input3).unwrap();
        assert_eq!(opr, String::from("get"));
        assert_eq!(check_res, Some(true));

        // コマンドと引数2つを受け取るケース
        let input4: String = String::from("put k1 value1");
        let (opr, check_res) = crate::check_input(&input4).unwrap();
        assert_eq!(opr, String::from("put"));
        assert_eq!(check_res, Some(true));

        // コマンドと引数3つを受け取るケース(エラー)
        let input5: String = String::from("put k1 value1 error");
        let (opr, check_res) = crate::check_input(&input5).unwrap();
        assert_eq!(opr, String::from("put"));
        assert_eq!(check_res, Some(false));
    }

    #[test]
    fn test_check_args() {
        // put のケース
        assert_eq!(crate::check_args("put", 1), Ok(false));
        assert_eq!(crate::check_args("put", 2), Ok(true));
        assert_eq!(crate::check_args("put", 3), Ok(false));

        // get, delete のケース
        assert_eq!(crate::check_args("get", 1), Ok(true));
        assert_eq!(crate::check_args("get", 2), Ok(false));
        assert_eq!(crate::check_args("delete", 1), Ok(true));
        assert_eq!(crate::check_args("delete", 2), Ok(false));

        // exit のケース
        assert_eq!(crate::check_args("exit", 0), Ok(true));
        assert_eq!(crate::check_args("exit", 1), Ok(false));

        // 他のコマンドのケース(エラー)
        assert_eq!(
            crate::check_args("error", 0),
            Err(crate::CommandError::CommandNotDefine("error".to_string()))
        )
    }
}
