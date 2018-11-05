use error::ForkliftResult;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn test_read_file_lines() {
    let testvec = match read_file_lines(Path::new("notnodes.txt")) {
        Ok(n) => n,
        Err(e) => {
            error!("This branch should not have been accessed {}", e);
            Vec::new()
        }
    };

    let vec = vec![
        "172.17.0.2:5671",
        "172.17.0.3:1234",
        "172.17.0.4",
        "172.17.0.1:7654",
    ];
    assert_eq!(testvec, vec);
}

/**
 * read_file_lines: &str -> ForkliftResult<Vec<String>>
 * REQUIRES: filename is a valid, non-empty file name
 * ENSURES: returns OK(Vec<String>) where the Vec contains the lines of the input file,
 * otherwise returns a ForkliftError if an I/O error occurs, or if the input file is not
 * valid UTF-8 character format. It fails outright if a line cannot be parsed into a String.
 */
pub fn read_file_lines(filename: &Path) -> ForkliftResult<Vec<String>> {
    debug!("Attempting to open input file {:?}", filename);
    let reader = BufReader::new(File::open(filename)?);
    let node_list: Vec<String> = reader
        .lines()
        .map(|l| {
            trace!("Parsing line '{:?}' from file to string", l);
            l.expect("Could not parse line from file to string")
        }).collect::<Vec<String>>();
    debug!(
        "Parsing file to address string list ok! String list: {:?}",
        node_list
    );
    Ok(node_list)
}

#[test]
fn test_current_time_in_millis() {
    let start = current_time_in_millis(SystemTime::now()).unwrap();
    ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    let end = current_time_in_millis(SystemTime::now()).unwrap();
    println!("Time difference {}", end - start);
    assert!(end - start < 1002 && end - start >= 1000);
}

/*
    current_time_in_millis: SystemTime -> u64
    REQUIRES: start is the current System Time
    ENSURES: returns the time since the UNIX_EPOCH in milliseconds
*/
pub fn current_time_in_millis(start: SystemTime) -> ForkliftResult<u64> {
    let since_epoch = start.duration_since(UNIX_EPOCH)?;
    trace!("Time since epoch {:?}", since_epoch);
    Ok(since_epoch.as_secs() * 1000 + u64::from(since_epoch.subsec_nanos()) / 1_000_000)
}

/*
    get_port_from_fulladdr: &str -> ForkliftResult<String>
    REQUIRES: full_address the full ip:port address
    ENSURES: returns Ok(port) associated with the input full address, otherwise
    return Err (in otherwords, the full_address is improperly formatted)
*/
pub fn get_port_from_fulladdr(full_address: &str) -> ForkliftResult<String> {
    trace!(
        "Attempt to parse address {} into socket to get port number",
        full_address
    );
    let addr = full_address.parse::<SocketAddr>()?;
    trace!(
        "Successfully parsed address {} into socket {:?}",
        full_address,
        addr
    );
    Ok(addr.port().to_string())
}
