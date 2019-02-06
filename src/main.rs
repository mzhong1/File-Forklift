use clap::*;
use clap::{App, Arg};
use crossbeam::channel;
use log::*;
use nanomsg::{Protocol, Socket};
use rendezvous_hash::{DefaultNodeHasher, RendezvousNodes};
use simplelog::{CombinedLogger, Config, SharedLogger, TermLogger, WriteLogger};

use std::fs::File;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};

mod cluster;
mod console_output;
mod error;
mod filesystem;
mod filesystem_entry;
mod filesystem_ops;
mod input;
mod local_ip;
mod message;
mod node;
mod progress_message;
mod progress_worker;
mod pulse;
mod rsync;
mod rsync_worker;
mod socket_node;
mod walk_worker;

use crate::cluster::Cluster;
use crate::console_output::ConsoleProgressOutput;
use crate::error::ForkliftResult;
use crate::input::*;
use crate::node::*;
use crate::rsync::*;
use crate::socket_node::*;

#[test]
fn test_init_router() {
    match init_router(&SocketAddr::new(
        ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(10, 26, 24, 92)),
        5555,
    )) {
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
fn init_router(full_address: &SocketAddr) -> ForkliftResult<Socket> {
    debug!("Initializing router");
    let mut router = Socket::new(Protocol::Bus)?;
    debug!("New router bus created");
    let current_port = full_address.port();
    router.bind(&format!("tcp://*:{}", current_port))?;
    debug!("router bound to port {}", current_port);
    Ok(router)
}

fn parse_matches(matches: &clap::ArgMatches<'_>) -> ForkliftResult<Input> {
    let path = match matches.value_of("config") {
        None => Path::new(""),
        Some(t) => Path::new(t),
    };
    let input = std::fs::read_to_string(path)?;
    Input::new(&input)
}

fn heartbeat(
    node_names: NodeList,
    joined: &mut bool,
    full_address: SocketAddr,
    s: crossbeam::Sender<ChangeList>,
) -> ForkliftResult<()> {
    let mess = ChangeList::new(ChangeType::AddNode, SocketNode::new(full_address));
    s.send(mess);

    let router = match init_router(&full_address) {
        Ok(t) => t,
        Err(e) => {
            error!("Error {:?}, Unable to connect router!", e);
            panic!("Error {:?}, Unable to connect router!", e)
        }
    }; //Make the node
    std::thread::sleep(std::time::Duration::from_millis(10));
    let mut cluster = Cluster::new(router, &full_address, s);
    cluster.nodes = NodeMap::init_nodemap(&full_address, cluster.lifetime, &node_names.node_list); //create mutable hashmap of nodes
                                                                                                   //sleep for a bit to let other nodes start up
    cluster.names = node_names;
    cluster.init_connect(&full_address);
    cluster.heartbeat_loop(&full_address, joined)
}

fn init_logs(f: &Path, level: simplelog::LevelFilter) -> ForkliftResult<()> {
    if !f.exists() {
        File::create(f)?;
    }
    let mut loggers: Vec<Box<dyn SharedLogger>> = vec![];
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
            Arg::with_name("config")
                .help("The name of the JSON file storing the cluster configuration for the node")
                .long_help("The name of the JSON file storing the cluster configurations for the node, formatted in JSON as nodes: [SocketAddresses], src_server: 'name of source server', dest_server: 'name of destination server', src_share: 'name of source share', dest_share: 'name of destination share'")
                .long("config")
                .short("c")
                .takes_value(true)
                .value_name("CONFIGFILE")
                .number_of_values(1)
                .required(true)
        ).arg(
            Arg::with_name("username")
                .help("The username of the owner of the share")
                .long("username")
                .short("u")
                .takes_value(true)
                .value_name("USERNAME")
                .number_of_values(1)
                .required(true)
        ).arg(
            Arg::with_name("password")
                .help("The password of the owner of the share")
                .long("password")
                .short("p")
                .takes_value(true)
                .value_name("PASSWORD")
                .number_of_values(1)
                .required(true)
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
    let username = match matches.value_of("username") {
        Some(e) => {
            if e.is_empty() {
                "guest"
            } else {
                e
            }
        }
        None => {
            error!("username not found");
            panic!("username not found!")
        }
    };
    let password = match matches.value_of("password") {
        Some(e) => {
            if e.is_empty() {
                "\n"
            } else {
                e
            }
        }
        None => {
            error!("password not found");
            panic!("password not found!")
        }
    };
    init_logs(&path, level)?;
    debug!("Log path: {:?}", logfile);
    info!("Logs made");

    let (s, r) = channel::unbounded::<ChangeList>();
    let mut active_nodes = Arc::new(Mutex::new(RendezvousNodes::default()));

    let input = parse_matches(&matches)?;
    if input.nodes.len() < 1 {
        panic!(
            "No input nodes!  Please have at least 1 node in the nodes section of your
        config file"
        );
    }
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
    let nodes = input.nodes.clone();
    let node_names: NodeList = NodeList::new_with_list(nodes);
    let full_address = match node_names.get_full_address(&ip_address.to_string()) {
        Some(a) => a,
        None => {
            error!("ip address {} not in the node_list ", ip_address);
            panic!("ip address {} not in the node_list ", ip_address)
        }
    };
    debug!("current full address: {:?}", full_address);
    let mine = SocketNode::new(full_address);
    let mut joined = input.nodes.len() != 1;
    let console_info = ConsoleProgressOutput::new();
    let system = input.system;
    let syncer = Rsyncer::new(system, Box::new(console_info));

    let stats = syncer.sync(
        (&input.src_server, &input.dest_server),
        (&input.src_share, &input.dest_share),
        (input.debug_level, input.num_threads),
        (input.workgroup, username.to_string(), password.to_string()),
        active_nodes.clone(),
        mine,
    );

    rayon::join(
        || heartbeat(node_names, &mut joined, full_address, s),
        || rendezvous(&mut active_nodes, &r),
    );
    Ok(())
}

/**
 * Thread where rendezvous hash is dealt with
 */
fn rendezvous(
    active_nodes: &mut Arc<Mutex<RendezvousNodes<SocketNode, DefaultNodeHasher>>>,
    r: &crossbeam::Receiver<ChangeList>,
) {
    loop {
        let mut list = active_nodes.lock().unwrap();
        match r.try_recv() {
            Ok(c) => {
                match c.change_type {
                    ChangeType::AddNode => {
                        debug!("Add Node {:?} to active list!", c.socket_node);
                        list.insert(c.socket_node);
                        debug!(
                            "The current list is {:?}",
                            list.calc_candidates(&1).collect::<Vec<_>>()
                        );
                    }
                    ChangeType::RemNode => {
                        debug!("Remove Node {:?} from active list!", c.socket_node);
                        //let list = Arc::get_mut(active_nodes).unwrap();
                        list.remove(&c.socket_node);
                        debug!(
                            "The current list is {:?}",
                            list.calc_candidates(&1).collect::<Vec<_>>()
                        );
                    }
                };
            }
            Err(_) => trace!("No Changes"),
        }
    }
}
