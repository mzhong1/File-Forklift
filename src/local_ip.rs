use nix;

use self::nix::ifaddrs::getifaddrs;
use crate::error::{ForkliftError, ForkliftResult};
use crate::postgres_logger::{send_mess, LogMessage};
use crate::tables::*;

use crossbeam::channel::Sender;
use log::*;
use std::fs::File;
use std::io::{BufRead, BufReader, Result};
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::path::Path;

// get the default gateway ip address
fn get_default_v4_iface() -> Result<Option<String>> {
    let p = Path::new("/proc/net/route");
    trace!("Try to open path /proc/net/route");
    let proc_route = File::open(p)?;
    let reader = BufReader::new(proc_route);
    trace!("Read in lines from /proc/net/route");
    for line in reader.lines() {
        let l = line?;
        let parts: Vec<&str> = l.split_whitespace().collect();
        trace!("parts {:?}", parts);
        if parts.len() > 2 && parts[1] == "00000000" {
            //Default gateway found
            return Ok(Some(parts[0].to_string()));
        }
    }

    Ok(None)
}

/// Get the local ip address of the default route
/// on this machine
pub fn get_ip(send_log: &Sender<LogMessage>) -> ForkliftResult<Option<SocketAddr>> {
    let default_iface = get_default_v4_iface()?;
    let default_iface = default_iface.unwrap();
    trace!("Default interface: {:?}", default_iface);
    let addrs = getifaddrs().unwrap();
    trace!("Loop through addresses in the default interface");
    for ifaddr in addrs {
        if ifaddr.interface_name == default_iface {
            // We found it
            match ifaddr.address {
                Some(address) => {
                    trace!("interface {} address {}", ifaddr.interface_name, address);
                    if let Ok(ip) = address.to_str().parse::<SocketAddrV4>() {
                        debug!("IP: {}", ip);
                        return Ok(Some(SocketAddr::from(ip)));
                    }
                }
                None => {
                    let mess = LogMessage::ErrorType(
                        ErrorType::IpLocalError,
                        format!(
                            "interface {} with unsupported address family",
                            ifaddr.interface_name
                        ),
                    );
                    send_mess(mess, &send_log)?;
                }
            }
        }
    }
    Err(ForkliftError::IpLocalError("Could not determine local ip address".to_string()))
}

//get the ip in ipv6.
pub fn get_ipv6() -> ForkliftResult<Option<SocketAddr>> {
    let default_iface = get_default_v4_iface()?;
    let default_iface = default_iface.unwrap();
    debug!("Default interface: {:?}", default_iface);
    let addrs = getifaddrs().unwrap();
    trace!("Loop through addresses in the default interface");
    for ifaddr in addrs {
        if ifaddr.interface_name == default_iface {
            // We found it
            match ifaddr.address {
                Some(address) => {
                    trace!("interface {} address {}", ifaddr.interface_name, address);
                    if let Ok(ip) = address.to_str().parse::<SocketAddrV6>() {
                        debug!("IP: {}", ip);
                        return Ok(Some(SocketAddr::from(ip)));
                    }
                }
                None => {
                    error!("interface {} with unsupported address family", ifaddr.interface_name);
                }
            }
        }
    }
    Err(ForkliftError::IpLocalError("Could not determine local ip address".to_string()))
}

#[test]
fn test_get_ipv6() {
    let socketa = get_ipv6().unwrap().unwrap();
    println!("socket: {:?}", socketa);
}
