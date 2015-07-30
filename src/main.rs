extern crate getopts;
extern crate time;
extern crate postgres;

use getopts::{optopt,optflag,getopts,OptGroup};

use std::os;
use std::io::fs::PathExtensions;
use std::io::BufferedReader;
use std::io::File;

use std::fmt;

use time::{Timespec, Tm};

use postgres::{PostgresConnection, NoSsl};
use postgres::types::ToSql;

struct Entry {
      bucket_owner:     String,
      bucket:           String,
      time:             Tm,
      remote_ip:        String,
      requester:        String,
      request_id:       String,
      operation:        String,
      aws_key:          String,
      request_uri:      String,
      status_code:      String,
      error_code:       String,
      bytes_sent:       i32,
      object_size:      i32,
      total_time:       i32,
      turn_around_time: i32,
      referrer:         String,
      user_agent:       String,
      version_id:       String,
}

impl Entry {
    fn new(values: Vec<String>) -> Entry {
        if values.len() <= 17 { println!("Incorrect number of fields in: {}", values); }
        //println!("{}", values.len());
        Entry {
            bucket_owner:     values[0].to_string(),
            bucket:           values[1].to_string(),
            time:             time::strptime(values[2].as_slice(), "%d/%b/%Y:%H:%M:%S %z").unwrap(),
            remote_ip:        values[3].to_string(),
            requester:        values[4].to_string(),
            request_id:       values[5].to_string(),
            operation:        values[6].to_string(),
            aws_key:          values[7].to_string(),
            request_uri:      values[8].to_string(),
            status_code:      values[9].to_string(),
            error_code:       values[10].to_string(),
            bytes_sent:       from_str(values[11].as_slice()).unwrap_or(0),
            object_size:      from_str(values[12].as_slice()).unwrap_or(0),
            total_time:       from_str(values[13].as_slice()).unwrap_or(0),
            turn_around_time: from_str(values[14].as_slice()).unwrap_or(0),
            referrer:         values[15].to_string(),
            user_agent:       values[16].to_string(),
            version_id:       values[17].to_string(),
        }
    }
}

impl fmt::Show for Entry {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "ENTRY[\"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\"]", self.bucket_owner, self.bucket, self.time, self.remote_ip, self.requester, self.request_id, self.operation, self.aws_key, self.request_uri, self.status_code, self.error_code, self.bytes_sent, self.object_size, self.total_time, self.turn_around_time, self.referrer, self.user_agent, self.version_id )
    }
}

fn insert_entry(conn: &PostgresConnection, entry: &Entry) {
    let statement = conn.prepare("insert into entries (bucket_owner, bucket, time, remote_ip, requester, request_id, operation, aws_key, request_uri, status_code, error_code, bytes_sent, object_size, total_time, turn_around_time, referrer, user_agent, version_id) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)").unwrap();
    
    let result = statement.execute(&[
        &entry.bucket_owner,
        &entry.bucket,
        &entry.time.to_timespec(),
        &entry.remote_ip,
        &entry.requester,
        &entry.request_id,
        &entry.operation,
        &entry.aws_key,
        &entry.request_uri,
        &entry.status_code,
        &entry.error_code,
        &entry.bytes_sent,
        &entry.object_size,
        &entry.total_time,
        &entry.turn_around_time,
        &entry.referrer,
        &entry.user_agent,
        &entry.version_id,
    ]);
    match result {
        Ok(x) => (),
        Err(x) => println!("{} for entry {}", x, &entry),
    };
}

fn count_rows(conn: &PostgresConnection) {
    let statement = conn.prepare("select count(*) from entries;").unwrap();
    for res in statement.query([]).unwrap() {
        let result : Option<i64> = res.get(0);
        match result {
            Some(x) => println!("Derp? {}", x),
            None => println!("What went wrong?! {}", result),
        }
    }
}

fn main() {
    let args: Vec<String> = os::args();
    if args.len() > 1 {
        let path = Path::new(args[1].clone());
        if path.exists() {
            println!("Log file exists at \"{}\"", path.display());
            let mut file = BufferedReader::new(File::open(&path));

            let conn = PostgresConnection::connect("postgresql://ted@localhost:5432/dcloud_s3_analytics",&NoSsl).unwrap();

            let mut count = 0u;
            let mut entries = Vec::new();
            let mut tokens = Vec::new();
            let mut token = String::new();
            let mut terminator = ' ';
            let mut skip_next = false;
            for c_wrap in file.chars() {
                let c = c_wrap.unwrap();

                // Newline always terminates the line & resets the terminator.
                if c == '\n' {
                    //println!("Final line token: \"{}\", and terminator is: '{}'", token, terminator);
                    tokens.push(token.clone());
                    if !(tokens.len() < 17) { 
                        entries.push(Entry::new(tokens.clone())); 
                    } else {
                        println!("Parsing error for: {}", tokens);
                    }
                    token.truncate(0);
                    //println!("{}", tokens);
                    tokens.truncate(0);
                    if entries.len() >= 10000 {
                        for entry in entries.iter() { insert_entry(&conn, entry); }
                        entries.truncate(0);
                        count += 10000;
                        //count_rows(&conn);
                        //println!("Running total: {}", total);
                    }
                    terminator = ' '; // guarantee that a newline terminates the line.
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
            for entry in entries.iter() { insert_entry(&conn, entry); }
            //count_rows(&conn);
        } else {
            println!("Log file doesn't exist at \"{}\"", path.display());
        }
    } else {
        println!("You must supply a path to log file");
    }
}
