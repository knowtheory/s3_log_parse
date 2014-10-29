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
      bytes_sent:       String,
      object_size:      String,
      total_time:       String,
      turn_around_time: String,
      referrer:         String,
      user_agent:       String,
      version_id:       String,
}

fn entry_for(values: Vec<String>) -> Entry {
    if values.len() <= 17 { println!("{}", values); }
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
        bytes_sent:       values[11].to_string(),
        object_size:      values[12].to_string(),
        total_time:       values[13].to_string(),
        turn_around_time: values[14].to_string(),
        referrer:         values[15].to_string(),
        user_agent:       values[16].to_string(),
        version_id:       values[17].to_string(),
    }
}

fn process(entry: &Entry) -> uint {
    let value = from_str::<uint>(entry.bytes_sent.as_slice()).unwrap_or(0u);
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

            let mut total = 0u;
            let mut entries = Vec::new();
            let mut tokens = Vec::new();
            let mut token = String::new();
            let mut terminator = ' ';
            let mut skip_next = false;
            for c_wrap in file.chars() {
                let c = c_wrap.unwrap();

                if c == '\n' {
                    tokens.push(token.clone());
                    entries.push(entry_for(tokens.clone()));
                    token.truncate(0);
                    //println!("{}", tokens);
                    tokens.truncate(0);
                    if entries.len() == 100 {
                        for entry in entries.iter() { total += process(entry); }
                    }
                } else if skip_next {
                    skip_next = false;
                } else if c == terminator {
                    if terminator != ' ' { 
                        skip_next = true;
                        terminator = ' ';
                    }
                    //println!("\"{}\"",token);
                    tokens.push(token.clone());
                    token.truncate(0);
                } else if c == '[' {
                    terminator = ']';
                } else if c == '"' {
                    terminator = '"';
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
