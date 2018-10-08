#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;
#[macro_use]
extern crate clap;
extern crate api;
extern crate local_ip;
extern crate zmq;
extern crate dirs;
extern crate nanomsg;
extern crate simplelog;
extern crate serde;
extern crate serde_json;

use self::api::service_generated::*;
use clap::{App, Arg};
use nanomsg::{Error, PollFd, PollInOut, PollRequest, Protocol, Socket};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

mod error;
mod mess;
mod node;
mod node_address;

use node_address::NodeAddress;
use error::{ForkliftError, ForkliftResult, NodeError};
use node::Node;
use simplelog::{CombinedLogger, Config, SharedLogger, TermLogger, WriteLogger};

/*
    Heartbeat protocol
    In a worker (Dealer socket):
    Calculate liveness (how many missed heartbeats before assuming death)
    wait in poll loop one sec at a time
    if message from other worker?  router?  reset liveness
    if no message count down
    if liveness reaches zero, consider the node dead.
*/

/*
    current_time_in_millis: SystemTime -> u64
    REQUIRES: start is the current System Time
    ENSURES: returns the time since the UNIX_EPOCH in milliseconds
*/
fn current_time_in_millis(start: SystemTime) -> ForkliftResult<u64> {
    let since_epoch = start.duration_since(UNIX_EPOCH)?;
    //.expect("Time went backwards whoops");
    debug!("Time since epoch {:?}", since_epoch);
    Ok(since_epoch.as_secs() * 1000 + since_epoch.subsec_nanos() as u64 / 1_000_000)
}

/*
    init_node_names: &str -> ForkliftResult<Vec<String>>
    REQUIRES: filename is a valid Json File 
    ENSURES: returns the String vector of ip:port addresses wrapped in ForkliftResult,
    or returns an Error (IO error)
*/
fn init_node_names(filename: &str) -> ForkliftResult<Vec<NodeAddress>> {
    let mut reader = BufReader::new(File::open(filename)?);
    let mut s :String = String::new();
    reader.read_to_string(&mut s)?;
    let node_names : Vec<NodeAddress> = serde_json::from_str(&s)?;

    Ok(node_names)
}

/*
    get_full_address: &str * &mut Vec<NodeAddress> -> String
    REQUIRES: ip a valid ip address, node_names is not empty
    ENSURES: returns the ip:port associated with the input ip address
    that is stored in node_names, otherwise return an AddressNotFoundError
*/
fn get_full_address_from_ip(ip: &str, node_names: &mut Vec<NodeAddress>) -> ForkliftResult<String> {
    for n in node_names {
        if n.ip_address == ip {
            return Ok(n.full_address.to_owned());
        }
    }
    Err(ForkliftError::NodeNotFoundError(NodeError::AddressNotFoundError))
}

/*
    get_port_from_ip: &str * Vec<nodeAddress> -> String
    REQUIRES: ip an ip address, node_names is not empty
    ENSURES: returns the port associated with the input ip, otherwise
    return PortNotFoundError
*/
fn get_port_from_ip(ip: &str, node_names: &mut Vec<NodeAddress>) -> ForkliftResult<String> {
    for n in node_names {
        if n.ip_address == ip {
            return Ok(n.port.to_owned());
        }
    }
    Err(ForkliftError::NodeNotFoundError(NodeError::PortNotFoundError))
}

/*
    get_port_from_fulladdr: &str * Vec<nodeAddress> -> String
    REQUIRES: full_address the full ip:port address, node_names is not empty
    ENSURES: returns the port associated with the input full address, otherwise
    return PortNotFoundError
*/
fn get_port_from_fulladdr(full_address: &str, node_names: &mut Vec<NodeAddress>) -> ForkliftResult<String>{
    for n in node_names {
        if n.full_address == full_address {
            return Ok(n.port.to_owned());
        }
    }
    Err(ForkliftError::NodeNotFoundError(NodeError::PortNotFoundError))
}

/*
    get_ip_from_list: &str * Vec<nodeAddress> -> String
    REQUIRES: full address the full ip:port address, node_names is not empty
    ENSURES: returns the ip associated with the input full address, otherwise
    return IpNotFoundError
*/
fn get_ip_from_list(full_address: &str, node_names: &mut Vec<NodeAddress>) -> ForkliftResult<String>{
    for n in node_names{
        if n.full_address == full_address{
            return Ok(n.ip_address.to_owned());
        }
    }
    Err(ForkliftError::NodeNotFoundError(NodeError::IpNotFoundError))
}

/*
    nodenames_contain_full_addressL &str * &mut Vec<NodeAddress> -> bool
    REQUIRES: full_address is the full ip:port address, node_names not empty,
    ENSURES: returns true if the full address is in one of the NodeAddress elements of node_names,
    false otherwise
*/
fn nodenames_contain_full_address(full_address: &str, node_names: &mut Vec<NodeAddress>) -> bool {
    for n in node_names{
        if n.full_address == full_address{
            return true;
        }
    }
    false
}

/*
    add_node_to_list: &str * &mut Vec<NodeAddress> -> null
    REQUIRES: full_address is the full ip:port address, node_names not empty,
    ENSURES: adds a new node with the address of full_address to node_names, if not already
    in the vector, else it does nothing
*/
fn add_node_to_list(full_address: &str, node_names: &mut Vec<NodeAddress>)
{
    if !nodenames_contain_full_address(full_address, node_names)
    {
let temp_node = NodeAddress::from_full_address(full_address);
    node_names.push(temp_node);
    }
    
}

/*
    to_string_vector: &mut Vec<NodeAddress> -> Vec<String>
    REQUIRES: node_names not empty
    ENSURES: returns a vector of the fulladdresses stored in node_names,
    otherwise return an empty vector
*/
fn to_string_vector(node_names: &mut Vec<NodeAddress>) -> Vec<String>
{
    let mut names = Vec::new();
    for n in node_names{
        names.push(n.full_address.to_owned());
    }
    names
}

/*
   NOTE: Once using the JSON parser, might become redundant
   Actually, right now it looks pretty darn redundent
   //Delete soon please
*/
fn get_port_from_address(ip: &str, full_address: &str) -> String {
    let splitip = full_address.split(ip);
    let vec = splitip.collect::<Vec<&str>>();
    vec[vec.len() - 1].to_string()
}

fn make_nodemap(
    node_names: &Vec<NodeAddress>,
    full_address: &str,
    lifetime: i64,
) -> HashMap<String, Node> {
    //create mutable hashmapof nodes
    let mut nodes = HashMap::new();
    //fill in vectors with default values
    for node_ip in node_names {
        if node_ip.full_address != full_address {
            debug!("node ip addresses and port: {:?}", node_ip);
            let mut temp_node = Node::new(full_address, lifetime);
            debug!("Node successfully created : {:?}", &temp_node);
            nodes.insert(node_ip.full_address.to_string(), temp_node);
        }
    }
    nodes
}

fn add_node_to_map(
    nodes: &mut HashMap<String, Node>,
    full_node_ip: &str,
    lifetime: i64,
    heartbeat: bool,
) {
    if !nodes.contains_key(full_node_ip) {
        debug!("node ip addresses and port: {}", full_node_ip);
        let temp_node = Node::node_new(full_node_ip, lifetime, lifetime, heartbeat);
        debug!("Node successfully created : {:?}", &temp_node);
        nodes.insert(full_node_ip.to_string(), temp_node);
    }
}

fn make_and_add_node(
    node_names: &mut Vec<NodeAddress>,
    sent_address: &str,
    nodes: &mut HashMap<String, Node>,
    liveness: i64,
    heartbeat: bool,
    router: &mut Socket,
) {
    if  nodenames_contain_full_address(&sent_address.to_string(), node_names) {
        add_node_to_list(&sent_address, node_names);
        add_node_to_map(nodes, &sent_address, liveness, heartbeat);
        connect_node(&sent_address, router);
    }
}

fn init_router(full_address: &str, node_names: &mut Vec<NodeAddress>) -> ForkliftResult<Socket> {
    let mut router = Socket::new(Protocol::Bus)?;
    let current_port = get_port_from_fulladdr(full_address, node_names)?;
    assert!(router.bind(&format!("tcp://*:{}", current_port)).is_ok());
    Ok(router)
}

fn connect_node(full_node_ip: &str, router: &mut Socket) {
    let mut tcp: String = "tcp://".to_owned();
    tcp.push_str(full_node_ip);
    assert!(router.connect(&tcp).is_ok());
}

fn send_getlist(
    request: &PollRequest,
    heartbeat_at: &mut u64,
    name: &str,
    router: &mut Socket,
    interval: u64,
) {
    let c_time = match current_time_in_millis(SystemTime::now())
    {
        Ok(time) => time,
        Err(err) => {debug!("Time went backwards!"); panic!(err)}, //If time runs backwards well, PANIC
    };
    if request.get_fds()[0].can_write() && c_time > *heartbeat_at {
        let message = mess::create_message(MessageType::GETLIST, &vec![name.to_string()]);
        match router.nb_write(message.as_slice()) {
            Ok(..) => debug!("Getlist sent"),
            Err(Error::TryAgain) => debug!("Receiver not ready, message can't be sent"),
            Err(..) => debug!("Failed to write to socket!"),
        };
        *heartbeat_at = c_time + interval;
    }
}

fn send_nodelist(
    node_names: &mut Vec<NodeAddress>,
    msg_body: &Vec<String>,
    nodes: &mut HashMap<String, Node>,
    liveness: i64,
    router: &mut Socket,
) {
    let address_names = to_string_vector(node_names);
    let buffer = mess::create_message(MessageType::NODELIST, &address_names);

    if msg_body.len() > 0 {
        let sent_address = &msg_body[0];
        make_and_add_node(node_names, &sent_address, nodes, liveness, true, router);

        match router.nb_write(buffer.as_slice()) {
            Ok(_) => debug!("Node List sent!"),
            Err(Error::TryAgain) => debug!("Receiver not ready, message can't be sen't"),
            Err(err) => debug!("Problem while writing: {}", err),
        };
    }
}

fn send_heartbeat(name: &str, router: &mut Socket) {
    let buffer = vec![name.to_string()];
    let msg = mess::create_message(MessageType::HEARTBEAT, &buffer);
    match router.nb_write(msg.as_slice()) {
        Ok(_) => {
            println!("Heartbeat sent !");
        }
        Err(Error::TryAgain) => {
            println!("Receiver not ready, message can't be sent for the moment ...");
        }
        Err(err) => panic!("Problem while writing: {}", err),
    };
}

fn tickdown_nodes(nodes: &mut HashMap<String, Node>, node_list: &Vec<String>) {
    for i in node_list {
        nodes.entry(i.to_string()).and_modify(|n| {
            if !n.has_heartbeat {
                n.tickdown();
            } else {
                n.has_heartbeat = false;
            }
        });
    }
}

fn send_and_tickdown(
    request: &PollRequest,
    heartbeat_at: &mut u64,
    name: &str,
    router: &mut Socket,
    interval: u64,
    nodes: &mut HashMap<String, Node>,
    node_names: &mut Vec<NodeAddress>,
) {
    if request.get_fds()[0].can_write() {
        let c_time = match current_time_in_millis(SystemTime::now())
    {
        Ok(time) => time,
        Err(err) => {debug!("Time went backwards!"); panic!(err)}, //If time runs backwards well, PANIC
    };
        debug!("current time in millis {}", c_time);
        debug!("heartbeat_at {}", heartbeat_at);

        if c_time > *heartbeat_at {
            send_heartbeat(name, router);
            let address_names = to_string_vector(node_names);
            tickdown_nodes(nodes, &address_names);
            *heartbeat_at = c_time + interval
        }
    }
}

fn read_message_to_u8(router: &mut Socket) -> Vec<u8> {
    let mut buffer = Vec::new();
    match router.nb_read_to_end(&mut buffer) {
        Ok(_) => debug!("Read message {} bytes!", buffer.len()),
        Err(Error::TryAgain) => debug!("Nothing to be read"),
        Err(err) => debug!("Problem while reading: {}", err),
    };
    buffer
}

fn parse_nodelist_message(
    buf: &[u8],
    node_names: &mut Vec<NodeAddress>,
    nodes: &mut HashMap<String, Node>,
    liveness: i64,
    router: &mut Socket,
    has_nodelist: &mut bool
) {
    let list = match mess::read_message(buf) {
        Some(t) => t,
        None => vec![],
    };
    for l in list {
        make_and_add_node(node_names, &l, nodes, liveness, false, router)
    }
    *has_nodelist = true;
}


fn heartbeat_heard(
    msg_body: &Vec<String>,
    node_names: &mut Vec<NodeAddress>,
    nodes: &mut HashMap<String, Node>,
    liveness: i64,
    router: &mut Socket,
    full_address: &str,
) {
    if msg_body.len() > 0 {
        let sent_address = &msg_body[0];
        make_and_add_node(node_names, &sent_address, nodes, liveness, true, router);
        nodes
            .entry(full_address.to_string())
            .and_modify(|n| n.heartbeat());
    }
}

fn read_and_heartbeat(
    request: &PollRequest,
    router: &mut Socket,
    node_names: &mut Vec<NodeAddress>,
    nodes: &mut HashMap<String, Node>,
    liveness: i64,
    has_nodelist: &mut bool,
    heartbeat_at: &mut u64,
    full_address: &str,
    interval: u64,
) {
    if request.get_fds()[0].can_read() {
        //check message type
        let msg = read_message_to_u8(router);
        let msgtype = mess::get_message_type(&msg);
        let msg_body = match mess::read_message(&msg) {
            Some(t) => t,
            None => vec![],
        };
        if msgtype == MessageType::NODELIST {
            parse_nodelist_message(&msg, node_names, nodes, liveness, router, has_nodelist);
        }
        //if GETLIST message -> send list
        if msgtype == MessageType::GETLIST {
            send_nodelist(node_names, &msg_body, nodes, liveness, router);
        }
        //if HEARTBEAT update the nodes
        if msgtype == MessageType::HEARTBEAT {
            heartbeat_heard(
                &msg_body,
                node_names,
                nodes,
                liveness,
                router,
                &full_address,
            );
            if !*has_nodelist {
                send_getlist(request, heartbeat_at, full_address, router, interval);
            }
        }
        //otherwise we ignore the message
    }
}
/*
    if node_joined has been flagged, then we need to connect the node to the graph. 
    This is done by sending a GETLIST signal to the node that we are connected to
    every second until we get a NODELIST back. 
    Poll THIS machine's node
        Pollin using timeout of heartBeat interval
        if !has_nodelist:
            send GETLIST to connected nodes
        if can_read(): 
            if NODESLIST:
                unpack message to get list of nodes,
                update nodelist and nodes,
                connect to list of nodes
                set has_nodelist to true
            if GETLIST: 
                unpack message to get the sender address
                add sender to node_names + map
                send Nodelist to sender address
            if HEARTBEAT message from some socket 
            (ip address of the heartbeat sender):
                unpack message to find out sender
                if the sender is not in the list of nodes, add it to the node_names
                    and the node_map and connect
                update the liveness of the sender
                update had_heartbeat of node to true
        if can_write()
            if SystemTime > heartbeat_at:
                send HEARTBEAT
                loop through nodes in map
                    if node's had_heartbeat = true
                        reset had_heartbeat to false
                    else (had_heartbeat = false)
                        if liveness <= 0
                            assume node death
                            remove node from rendezvous
*/
fn heartbeat(
    router: &mut Socket,
    interval: u64,
    has_nodelist: &mut bool,
    heartbeat_at: &mut u64,
    full_address: &str,
    node_names: &mut Vec<NodeAddress>,
    nodes: &mut HashMap<String, Node>,
    liveness: i64,
) -> ForkliftResult<()> {
    loop {
        std::thread::sleep(std::time::Duration::from_millis(10));
        let mut items: Vec<PollFd> = vec![router.new_pollfd(PollInOut::InOut)];
        let mut request = PollRequest::new(&mut items);
        Socket::poll(&mut request, interval as isize)?;

        debug!("Poll can read: {:?}", request.get_fds()[0].can_read());
        println!("Poll can read: {:?}", request.get_fds()[0].can_read());

        if !*has_nodelist {
            send_getlist(&request, heartbeat_at, full_address, router, interval);
        }

        read_and_heartbeat(
            &request,
            router,
            node_names,
            nodes,
            liveness,
            has_nodelist,
            heartbeat_at,
            full_address,
            interval,
        );

        send_and_tickdown(
            &request,
            heartbeat_at,
            full_address,
            router,
            interval,
            nodes,
            node_names,
        );
    }
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
    loggers.push(WriteLogger::new(level, Config::default(), File::open(f)?));
    let _ = CombinedLogger::init(loggers);
    info!("Starting up");

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
    let path = match dirs::home_dir(){
        Some(path) => path.join("debuglog"),
        None => {debug!("Home directory not found"); panic!("Home Directory not found!")},
    };
    let path_str = path.to_string_lossy();
    let matches = App::new("Heartbeat Logs")
        .author(crate_authors!())
        .about("NFS and Samba filesystem migration program")
        .version(crate_version!())
        .arg(
            Arg::with_name("namelist")
                .help("Name of the file to pull the node list from")
                .long("namelist")
                .short("n")
                .takes_value(true)
                .required(true)
                .conflicts_with("join"),
        ).arg(
            Arg::with_name("logfile")
                .default_value(&path_str)
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
            Arg::from_usage(
                "-j, --join <YOURIP:PORT> 
            'The ip address and port of your node in the form of IP:PORT' 
            <NODEIP:PORT> 'The ip address and port of a live node to connect 
            to in the form IP:PORT'",
            ).required(false)
            .help("Joins a node not in the graph to the graph"),
        ).get_matches();
    let level = match matches.occurrences_of("v") {
        0 => simplelog::LevelFilter::Info,
        1 => simplelog::LevelFilter::Debug,
        _ => simplelog::LevelFilter::Trace,
    };
    let logfile = Path::new(matches.value_of("logfile").unwrap());
    init_logs(&logfile, level)?;
    //Variables that don't depend on command line args
    let liveness = 5; //The amount of times we can tick down before assuming death
    let interval = 1000; //set heartbeat interval in msecs
    let start = SystemTime::now();
    let mut heartbeat_at = current_time_in_millis(start)? + interval;

    let mut node_joined = false;
    let joined = match matches.values_of("join") {
        None => vec![],
        Some(t) => t.collect(),
    };

    let filename = match matches.value_of("namelist") {
        None => "",
        Some(t) => {
            node_joined = true;
            t
        }
    };

    let mut ip_address = match local_ip::get(){
        Some(ip) => ip.to_string(),
        None => {debug!("Unable to get ip address from ifconfig"); panic!("Unable to get ip address from ifconfig")},
    };
    let mut node_names: Vec<NodeAddress> = vec![];
    if joined.len() > 0 {
        let tempvec = joined[0].split(":").collect::<Vec<&str>>();
        ip_address = tempvec[0].to_string();
        add_node_to_list(joined[1], &mut node_names);
        add_node_to_list(joined[0], &mut node_names);
    } else {
        node_names = init_node_names(filename)?;
    }
    let mut nodes = make_nodemap(&node_names, &ip_address, liveness); //create mutable hashmap of nodes
    debug!("current ip address, port: {}", &ip_address);
    let mut router = init_router(&ip_address, &mut node_names)?; //Make the node
    debug!("router created");
    let full_address = get_full_address_from_ip(&ip_address, &mut node_names).unwrap(); //handle later

    //sleep for a bit to let other nodes start up
    std::thread::sleep(std::time::Duration::from_millis(10));

    //connect to addresses
    for node_ip in &mut node_names {
        if node_ip.full_address != full_address {
            connect_node(&node_ip.full_address, &mut router);
        }
    }
    debug!("Connection to nodes initiated");

    heartbeat(
        &mut router,
        interval,
        &mut node_joined,
        &mut heartbeat_at,
        &full_address,
        &mut node_names,
        &mut nodes,
        liveness,
    )?;
    Ok(())
}
