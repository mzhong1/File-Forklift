use error::ForkliftResult;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

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
