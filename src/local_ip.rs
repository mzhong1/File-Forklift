extern crate hex;
extern crate log;

use std::fs::File;
use std::io::{BufRead, BufReader, Error, ErrorKind, Result};
use std::net::{Ipv4Addr, SocketAddrV4, TcpStream, SocketAddr};
use std::path::Path;
use error::{ForkliftError, ForkliftResult};
#[test]
fn test() {
    println!("{:?}", get_ip());
}

// get the default gateway ip address
fn get_default_v4_route() -> Result<Option<Ipv4Addr>> {
    let p = Path::new("/proc/net/route");
    let proc_route = File::open(p)?;
    let reader = BufReader::new(proc_route);
    for line in reader.lines() {
        let l = line?;
        let parts: Vec<&str> = l.split_whitespace().collect();
        if parts.len() > 2 {
            // ipv4
            if parts[1] == "00000000" {
                let h = hex::decode(&parts[2].as_bytes())
                    .map_err(|e| Error::new(ErrorKind::Other, e))?;
                if h.len() != 4 {
                    return Err(Error::new(
                        ErrorKind::Other,
                        format!("Error converting {} from hex", parts[2]),
                    ));
                }
                //Default gateway found
                return Ok(Some(Ipv4Addr::new(h[3], h[2], h[1], h[0])));
            }
        }
    }

    Ok(None)
}

/// Get the local ip address of the default route
/// on this machine
pub fn get_ip() -> ForkliftResult<Option<SocketAddr>> {
    let default_addr = get_default_v4_route()?;
    match default_addr {
        Some(addr) => {
            let s_addr = SocketAddrV4::new(addr, 53);
            let s = TcpStream::connect(s_addr)?;
            let p = s.local_addr()?;
            println!("p {:?}", p);
            return Ok(Some(p));
        },
        None => {}
    };
    Err(ForkliftError::IpLocalError)
}
