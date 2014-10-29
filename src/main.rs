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

fn process(values: Vec<String>) -> Entry {
    println!("{}", values.len());
    let entry = Entry {
        bucket_owner:     values[0].clone(),
        bucket:           values[1].clone(),
        time:             values[2].clone(),
        remote_ip:        values[3].clone(),
        requester:        values[4].clone(),
        request_id:       values[5].clone(),
        operation:        values[6].clone(),
        aws_key:          values[7].clone(),
        request_uri:      values[8].clone(),
        status_code:      values[9].clone(),
        error_code:       values[10].clone(),
        bytes_sent:       values[11].clone(),
        object_size:      values[12].clone(),
        total_time:       values[13].clone(),
        turn_around_time: values[14].clone(),
        referrer:         values[15].clone(),
        user_agent:       values[16].clone(),
        version_id:       values[17].clone(),
    };
    println!("{}", entry.request_uri);
    entry
}

fn main() {
    let path = Path::new("/Users/ted/data/dc/aws_usage/access/test.log");
    let mut file = BufferedReader::new(File::open(&path));

    let mut tokens = Vec::new();
    let mut token = String::new();
    let mut terminator = ' ';
    let mut skip_next = false;
    for c_wrap in file.chars() {
    let c = c_wrap.unwrap();

    if c == '\n' {
        tokens.push(token.clone());
        process(tokens.clone());
        token.truncate(0);
        //println!("{}", tokens);
        tokens.truncate(0);
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
}
