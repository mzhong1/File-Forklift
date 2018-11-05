#[macro_use]
extern crate log;
#[macro_use]
extern crate clap;
extern crate api;
extern crate dirs;
extern crate nanomsg;
extern crate simplelog;

use clap::{App, Arg};
use nanomsg::{Protocol, Socket};
use std::fs::File;
use std::path::{Path, PathBuf};

mod cluster;
mod error;
mod local_ip;
mod message;
mod node;
mod pulse;
mod utils;

use cluster::Cluster;
use error::ForkliftResult;
use node::*;
use simplelog::{CombinedLogger, Config, SharedLogger, TermLogger, WriteLogger};

#[test]
fn test_init_router() {
    match init_router("10.26.24.92:5555") {
        Ok(s) => s,
        Err(e) => {
            error!("Error {}", e);
            panic!("Router cannot bind to port")
        }
    };
}

/**
 * init_router: &str -> ForkliftResult<Socket>
 * REQUIRES: full_address a string in the form ip:port, where
 * ip is your local ip and port is the port your node will bind to
 * ENSURES: returns a Result<Socket,Err> where if successful, returns
 * a new socket with the Bus Protocol bound to the input port.  Otherwise,
 * return the associated ForkliftError
 */
fn init_router(full_address: &str) -> ForkliftResult<Socket> {
    debug!("Initializing router");
    let mut router = Socket::new(Protocol::Bus)?;
    debug!("New router bus created");
    let current_port = utils::get_port_from_fulladdr(full_address)?;
    router.bind(&format!("tcp://*:{}", current_port))?;
    debug!("router bound to port {}", current_port);
    Ok(router)
}

fn parse_matches(matches: &clap::ArgMatches) -> (Vec<String>, PathBuf, bool) {
    let mut has_nodelist = false;
    let joined = match matches.values_of("join") {
        None => vec![],
        Some(t) => t.map(|e| e.to_string()).collect(),
    };

    let filename = match matches.value_of("namelist") {
        None => Path::new(""),
        Some(t) => {
            has_nodelist = true;
            Path::new(t)
        }
    };
    if joined.is_empty() {
        has_nodelist = true;
    }
    (joined, filename.to_path_buf(), has_nodelist)
}

fn heartbeat(matches: &clap::ArgMatches) -> ForkliftResult<()> {
    trace!("Attempting to get local ip address");
    let ip_address = match local_ip::get_ip() {
        Ok(Some(ip)) => ip.ip(),
        Ok(None) => {
            error!("No local ip! ABORT!");
            panic!("No local ip! ABORT!")
        }
        Err(e) => {
            error!("Error: {}", e);
            panic!("Error: {}", e)
        }
    };

    let (joined, filename, mut has_nodelist) = parse_matches(matches);

    let node_names: NodeList = NodeList::init_names(joined, &filename);
    let full_address = match node_names.get_full_address(&ip_address.to_string()) {
        Some(a) => a,
        None => {
            error!("ip address {} not in the node_list ", ip_address);
            panic!("ip address {} not in the node_list ", ip_address)
        }
    };
    debug!("current full address: {:?}", full_address);

    let router = init_router(&full_address.to_string())?; //Make the node
    std::thread::sleep(std::time::Duration::from_millis(10));
    let mut cluster = Cluster::new(router);
    cluster.nodes = NodeMap::init_nodemap(&full_address, cluster.lifetime, &node_names.node_list); //create mutable hashmap of nodes
                                                                                                   //sleep for a bit to let other nodes start up
    cluster.names = node_names;
    cluster.init_connect(&full_address);
    cluster.heartbeat_loop(&full_address, &mut has_nodelist)
    //Ok(())
}

fn init_logs(f: &Path, level: simplelog::LevelFilter) -> ForkliftResult<()> {
    if !f.exists() {
        File::create(f)?;
    }
    let mut loggers: Vec<Box<SharedLogger>> = vec![];
    if let Some(term_logger) = TermLogger::new(level, Config::default()) {
        loggers.push(term_logger);
    }
    loggers.push(WriteLogger::new(level, Config::default(), File::create(f)?));
    let _ = CombinedLogger::init(loggers);

    Ok(())
}

/*
    main takes in two flags: 
    j: computer is a new node, not a part of the original list
    d: create debug logs
    When the 'j' flag is raised, the program takes in the arguments ip_addr:port, otherip_addr:port
    Without the 'j' flag, the program takes in a file argument of ip_addr:port 
    addresses of all nodes in the graph
*/
fn main() -> ForkliftResult<()> {
    let matches = App::new("Heartbeat Logs")
        .author(crate_authors!())
        .about("NFS and Samba filesystem migration program")
        .version(crate_version!())
        .arg(
            Arg::with_name("namelist")
                .help("The name of the file storing the nodes in the cluster formatted so that each 
                node's ip:port is on a separate line")
                .long("namelist")
                .short("n")
                .takes_value(true)
                .value_name("NODESOCKETFILE")
                .number_of_values(1)
                .required(true)
                .conflicts_with("join"),
        ).arg(
            Arg::with_name("logfile")
                .default_value("debuglog")
                .help("Logs debug statements to file debuglog")
                .long("logfile")
                .short("l")
                .takes_value(true)
                .required(false),
        ).arg(
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the level of verbosity"),
        ).arg(
            Arg::with_name("join")
                .long("join")
                .short("j")
                .takes_value(true)
                .number_of_values(2)
                .value_names(&["YOUR IP:PORT", "NODE IP:PORT"])
                .long_help("Your IP:PORT is your node's socket value in the form ip address:port number, 
                while NODE IP:PORT is the ip:port of the node you are connecting to in the same format.")
                .required(false),
        ).get_matches();
    let level = match matches.occurrences_of("v") {
        0 => simplelog::LevelFilter::Info,
        1 => simplelog::LevelFilter::Debug,
        _ => simplelog::LevelFilter::Trace,
    };
    let logfile = matches.value_of("logfile").unwrap();
    let path = match dirs::home_dir() {
        Some(path) => path.join(logfile),
        None => {
            error!("Home directory not found");
            panic!("Home Directory not found!")
        }
    };
    //let path_str = path.to_string_lossy();
    init_logs(&path, level)?;
    debug!("Log path: {:?}", logfile);
    info!("Logs made");
    heartbeat(&matches)?;
    Ok(())
}
