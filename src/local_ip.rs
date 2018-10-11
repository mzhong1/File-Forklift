extern crate log;
extern crate nix;

use self::nix::ifaddrs::{getifaddrs};
use error::{ForkliftError, ForkliftResult};

use std::fs::File;
use std::io::{BufRead, BufReader, Result};
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::path::Path;

// get the default gateway ip address
fn get_default_v4_iface() -> Result<Option<String>> {
    let p = Path::new("/proc/net/route");
    let proc_route = File::open(p)?;
    let reader = BufReader::new(proc_route);
    for line in reader.lines() {
        let l = line?;
        let parts: Vec<&str> = l.split_whitespace().collect();
        if parts.len() > 2 {
            // ipv4
            if parts[1] == "00000000" {
                //Default gateway found
                return Ok(Some(parts[0].to_string()));
            }
        }
    }

    Ok(None)
}

/// Get the local ip address of the default route
/// on this machine
pub fn get_ip() -> ForkliftResult<Option<SocketAddr>> {
    let default_iface = get_default_v4_iface()?;
    let default_iface = default_iface.unwrap();
    println!("Default interface: {:?}", default_iface);
    let addrs = getifaddrs().unwrap();
    for ifaddr in addrs {
        if ifaddr.interface_name == default_iface {
            // We found it
            match ifaddr.address {
                Some(address) => {
                    println!("interface {} address {}", ifaddr.interface_name, address);
                    match address.to_str().parse::<SocketAddrV4>(){
                        Ok(ip) => {println!("IP: {}", ip); return Ok(Some(SocketAddr::from(ip)))},
                        Err(_) => (),
                    };
                },
                None => {
                    println!(
                        "interface {} with unsupported address family",
                        ifaddr.interface_name
                    );
                }
            }
         }
                
    }
    Err(ForkliftError::IpLocalError)
}     

pub fn get_ipv6() -> ForkliftResult<Option<SocketAddr>> {
    let default_iface = get_default_v4_iface()?;
    let default_iface = default_iface.unwrap();
    println!("Default interface: {:?}", default_iface);
    let addrs = getifaddrs().unwrap();
    for ifaddr in addrs {
        if ifaddr.interface_name == default_iface {
            // We found it
            match ifaddr.address {
                Some(address) => {
                    println!("interface {} address {}", ifaddr.interface_name, address);
                    match address.to_str().parse::<SocketAddrV6>(){
                        Ok(ip) => {println!("IP: {}", ip); return Ok(Some(SocketAddr::from(ip)))},
                        Err(_) => (),
                    };
                },
                None => {
                    println!(
                        "interface {} with unsupported address family",
                        ifaddr.interface_name
                    );
                }
            }
         }
                
    }
    Err(ForkliftError::IpLocalError)
} 

#[test]
fn test_get_ip() {
    let socketa = get_ip().unwrap().unwrap();
    println!("socket: {:?}", socketa);

    //assert_eq!(ip.to_string(), "10.26.24.92".to_string());
}

#[test]
fn test_get_ipv6() {
    let socketa = get_ipv6().unwrap().unwrap();
    println!("socket: {:?}", socketa);

    //assert_eq!(ip.to_string(), "10.26.24.92".to_string());
}