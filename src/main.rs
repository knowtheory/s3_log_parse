extern crate getopts;
use getopts::{optopt,optflag,getopts,OptGroup};
use std::os;
use std::io::fs::PathExtensions;
use std::io::BufferedReader;
use std::io::File;

struct Entry {
      bucket_owner:     String,
      bucket:           String,
      time:             String,
      remote_ip:        String,
      requester:        String,
      request_id:       String,
      operation:        String,
      aws_key:          String,
      request_uri:      String,
      status_code:      String,
      error_code:       String,
      bytes_sent:       uint,
      object_size:      uint,
      total_time:       uint,
      turn_around_time: uint,
      referrer:         String,
      user_agent:       String,
      version_id:       String,
}

fn entry_for(values: Vec<String>) -> Entry {
    if values.len() <= 17 { println!("Incorrect number of fields in: {}", values); }
    //println!("{}", values.len());
    Entry {
        bucket_owner:     values[0].to_string(),
        bucket:           values[1].to_string(),
        time:             values[2].to_string(),
        remote_ip:        values[3].to_string(),
        requester:        values[4].to_string(),
        request_id:       values[5].to_string(),
        operation:        values[6].to_string(),
        aws_key:          values[7].to_string(),
        request_uri:      values[8].to_string(),
        status_code:      values[9].to_string(),
        error_code:       values[10].to_string(),
        bytes_sent:       from_str::<uint>(values[11].as_slice()).unwrap_or(0u),
        object_size:      from_str::<uint>(values[12].as_slice()).unwrap_or(0u),
        total_time:       from_str::<uint>(values[13].as_slice()).unwrap_or(0u),
        turn_around_time: from_str::<uint>(values[14].as_slice()).unwrap_or(0u),
        referrer:         values[15].to_string(),
        user_agent:       values[16].to_string(),
        version_id:       values[17].to_string(),
    }
}

fn process(entry: &Entry) -> uint {
    let value = entry.bytes_sent;
    //println!("Value is: {}", value);
    value
}

fn main() {
    let args: Vec<String> = os::args();
    if args.len() > 1 {
        let path = Path::new(args[1].clone());
        if path.exists() {
            println!("Log file exists at \"{}\"", path.display());
            let mut file = BufferedReader::new(File::open(&path));

            let mut count = 0u;
            let mut total = 0u;
            let mut entries = Vec::new();
            let mut tokens = Vec::new();
            let mut token = String::new();
            let mut terminator = ' ';
            let mut skip_next = false;
            for c_wrap in file.chars() {
                let c = c_wrap.unwrap();

                if c == '\n' {
                    //println!("Final line token: \"{}\", and terminator is: '{}'", token, terminator);
                    tokens.push(token.clone());
                    if !(tokens.len() < 17) { 
                        entries.push(entry_for(tokens.clone())); 
                    } else {
                        println!("Parsing error for: {}", tokens);
                    }
                    token.truncate(0);
                    //println!("{}", tokens);
                    tokens.truncate(0);
                    if entries.len() >= 1000 {
                        for entry in entries.iter() { total += process(entry); }
                        entries.truncate(0);
                        count += 1000;
                        println!("Running Count: {}", count);
                        //println!("Running total: {}", total);
                    }
                    terminator = ' ' // guarantee that a newline terminates the line.
                } else if skip_next {
                    skip_next = false;
                } else if c == terminator {
                    //println!("Terminating token \"{}\" with '{}'", token, terminator)
                    //if c == '"' { println!("Closing Quote! (Token is: \"{}\" and terminator: '{}')", token, terminator); }
                    if terminator != ' ' { skip_next = true; }
                    terminator = ' ';
                    //println!("\"{}\"",token);
                    tokens.push(token.clone());
                    token.truncate(0);
                } else if c == '[' && terminator == ' ' {
                    terminator = ']';
                } else if c == '"' && terminator == ' '  {
                    //println!("WTF? '{}' ==? '{}'", terminator, c);
                    terminator = '"';
                    //println!("Open Quote! (Token is: \"{}\" and terminator: '{}')", token, terminator);
                } else {
                    token.push(c);
                }
            }
            for entry in entries.iter() { total += process(entry); }
            println!("Total: {}", total);
        } else {
            println!("Log file doesn't exist at \"{}\"", path.display());
        }
    } else {
        println!("You must supply a path to log file");
    }
}
