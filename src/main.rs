#[macro_use]
extern crate log;
#[macro_use]
extern crate clap;
extern crate api;
extern crate dirs;
extern crate nanomsg;
extern crate simplelog;

use self::api::service_generated::*;
use clap::{App, Arg};
use nanomsg::{Error, PollFd, PollInOut, PollRequest, Protocol, Socket};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::path::Path;

mod error;
mod local_ip;
mod message;
mod node;
mod pulse;

use error::{ForkliftError, ForkliftResult};
use node::Node;
use pulse::Pulse;
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

#[test]
fn test_read_file_lines() {
    let testvec = match read_file_lines("notnodes.txt") {
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
fn read_file_lines(filename: &str) -> ForkliftResult<Vec<String>> {
    debug!("Attempting to open input file {}", filename);
    let reader = BufReader::new(File::open(filename)?);
    let node_list: Vec<String> = reader
        .lines()
        .map(|l| {
            trace!("Parsing line '{:?}' from file to string", l);
            l.expect("Could not parse line from file to stirng")
        }).collect::<Vec<String>>();
    debug!(
        "Parsing file to address string list ok! String list: {:?}",
        node_list
    );
    Ok(node_list)
}

#[test]
fn test_init_node_names() {
    let wrong_filename = "nodes";
    match init_node_names(wrong_filename) //this should "break"
    {
        Ok(t) => {println!("{:?}", t); panic!("Should not go to this branch")},
        Err(e) => println!("Error {}", e),
    };

    let expected_result = vec![
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 17, 0, 2)),
            5671,
        ),
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 17, 0, 3)),
            1234,
        ),
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 17, 0, 4)),
            5555,
        ),
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 17, 0, 1)),
            7654,
        ),
    ];

    match init_node_names("nodes.txt") {
        Ok(t) => {
            println!("Expected: {:?}", expected_result);
            println!("Vec: {:?}", t);
            assert_eq!(expected_result, t)
        }
        Err(e) => {
            println!("Error {}", e);
            panic!("Should not end up in this branch")
        }
    }

    //this should "break"
    match init_node_names("notnodes.txt") {
        Ok(t) => {
            println!("{:?}", t);
            panic!("Should not go to this branch")
        }
        Err(e) => println!("Error {}", e),
    }
}
/*
    init_node_names: &str -> ForkliftResult<Vec<String>>
    REQUIRES: filename is a properly formatted File (each line has the ip:port of a node)
    ENSURES: returns the SocketAddr vector of ip:port addresses wrapped in ForkliftResult,
    or returns a ForkliftError (AddrParseError, since IO errors and file parsing errors
    will fail the program).
*/
fn init_node_names(filename: &str) -> ForkliftResult<Vec<SocketAddr>> {
    let node_list = match read_file_lines(filename) {
        Ok(n) => n,
        Err(e) => {
            error!(
                "Cannot read the node file. Error {}. Aborting program...",
                e
            );
            panic!(
                "Cannot read the input node file, error {}. Aborting program...",
                e
            )
        }
    };
    trace!("Attempting to collect parsed Socket Addresses to vector");
    let mut node_names: Vec<SocketAddr> = Vec::new();
    for n in node_list {
        trace!("parsing address {} to SocketAddress", n);
        node_names.push(n.parse::<SocketAddr>()?);
    }
    debug!(
        "Parsing file to socket list ok! Node list: {:?}",
        node_names
    );
    Ok(node_names)
}

#[test]
fn test_get_full_address_from_ip() {
    let mut names = vec![
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 17, 0, 2)),
            5671,
        ),
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 17, 0, 3)),
            1234,
        ),
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 17, 0, 4)),
            5555,
        ),
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 17, 0, 1)),
            7654,
        ),
    ];
    let expected_result = "172.17.0.4:5555".to_string();
    assert_eq!(
        Some(expected_result),
        get_full_address_from_ip("172.17.0.4", &mut names)
    );
    assert_eq!(None, get_full_address_from_ip("172.17.5.4", &mut names))
}
/*
    get_full_address_from_ip: &str * &mut Vec<SocketAddr> -> String
    REQUIRES: ip a valid ip address, node_names is not empty
    ENSURES: returns SOME(ip:port) associated with the input ip address
    that is stored in node_names, otherwise return NONE
*/
fn get_full_address_from_ip(ip: &str, node_names: &mut Vec<SocketAddr>) -> Option<String> {
    trace!(
        "Attempt to get full address of input ip {} from list of sockets",
        ip
    );
    for n in node_names {
        trace!("current loop address is {:?}", n);
        if n.ip().to_string() == ip {
            trace!("Successfully matched ip {} to full address {:?}", ip, n);
            return Some(n.to_string());
        }
    }
    trace!("failed to find a matching full address in for ip {}", ip);
    None
}

#[test]
fn test_get_port_from_fulladdr() {
    let full_addr = "123.23.42.11:1234";
    match get_port_from_fulladdr(full_addr) {
        Ok(t) => {
            println!("The port is: {}", t);
            assert_eq!("1234".to_string(), t)
        }
        Err(e) => {
            println!("Error: {:?}", e);
            panic!("This branch should not have been accessed")
        }
    }
    let fail_addr = "123.23.42.11";
    match get_port_from_fulladdr(fail_addr) {
        Ok(t) => {
            println!("The port is: {}", t);
            panic!("This branch should not have been accessed")
        }
        Err(e) => println!("Error: {:?}", e),
    }
}
/*
    get_port_from_fulladdr: &str -> ForkliftResult<String>
    REQUIRES: full_address the full ip:port address
    ENSURES: returns Ok(port) associated with the input full address, otherwise
    return Err (in otherwords, the full_address is improperly formatted)
*/
fn get_port_from_fulladdr(full_address: &str) -> ForkliftResult<String> {
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

#[test]
fn test_nodenames_contain_full_address() {
    let mut names = vec![
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 17, 0, 2)),
            5671,
        ),
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 17, 0, 3)),
            1234,
        ),
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 17, 0, 4)),
            5555,
        ),
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 17, 0, 1)),
            7654,
        ),
    ];
    assert_eq!(
        true,
        nodenames_contain_full_address("172.17.0.3:1234", &mut names)
    );
    assert_eq!(
        false,
        nodenames_contain_full_address("122.22.3.5:1234", &mut names)
    );
}
/*
    nodenames_contain_full_address &str * &mut Vec<SocketAddr> -> bool
    REQUIRES: full_address is the full ip:port address, node_names not empty,
    ENSURES: returns true if the full address is in one of the SocketAddr elements of node_names,
    false otherwise
*/
fn nodenames_contain_full_address(full_address: &str, node_names: &mut Vec<SocketAddr>) -> bool {
    node_names.iter().any(|n| n.to_string() == full_address)
}

#[test]
fn test_add_node_to_list() {
    let mut names = vec![
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 17, 0, 2)),
            5671,
        ),
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 17, 0, 3)),
            1234,
        ),
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 17, 0, 4)),
            5555,
        ),
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 17, 0, 1)),
            7654,
        ),
    ];

    let compare_names = vec![
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 17, 0, 2)),
            5671,
        ),
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 17, 0, 3)),
            1234,
        ),
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 17, 0, 4)),
            5555,
        ),
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 17, 0, 1)),
            7654,
        ),
    ];
    match add_node_to_list("172.17.0.3:1234", &mut names) {
        Ok(_t) => assert_eq!(names, compare_names),
        Err(e) => {
            println!("Error {}", e);
            panic!("This branch should not have been taken!")
        }
    }

    match add_node_to_list("122.22.3.5:1234", &mut names) {
        Ok(_t) => {
            assert_eq!(5, names.len());
            assert_ne!(names, compare_names);
            assert!(nodenames_contain_full_address(
                "122.22.3.5:1234",
                &mut names
            ))
        }
        Err(e) => {
            println!("Error {}", e);
            panic!("This branch should not have been taken!")
        }
    }

    match add_node_to_list("122.22.3.4", &mut names) {
        Ok(_t) => panic!("This branch should not have been taken!"),
        Err(e) => println!("Error {}", e),
    }
}
/*
    add_node_to_list: &str * &mut Vec<SocketAddr> -> null
    REQUIRES: full_address is the full ip:port address, node_names not empty,
    ENSURES: adds a new node with the address of full_address to node_names, if not already
    in the vector, else it does nothing
*/
fn add_node_to_list(full_address: &str, node_names: &mut Vec<SocketAddr>) -> ForkliftResult<()> {
    trace!(
        "Attempting to add address {} to list of sockets",
        full_address
    );
    if !nodenames_contain_full_address(full_address, node_names) {
        trace!(
            "Address {} not already in list, attempting to parse to socket",
            full_address
        );
        let temp_node = full_address.parse::<SocketAddr>()?;
        trace!(
            "Address {} successfully parsed to socket {:?}, pushing to list",
            full_address,
            temp_node
        );
        node_names.push(temp_node);
    }
    Ok(())
}

#[test]
fn test_to_string_vector() {
    let mut names = vec![
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 17, 0, 2)),
            5671,
        ),
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 17, 0, 3)),
            1234,
        ),
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 17, 0, 4)),
            5555,
        ),
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 17, 0, 1)),
            7654,
        ),
    ];
    let expected_result = vec![
        "172.17.0.2:5671".to_string(),
        "172.17.0.3:1234".to_string(),
        "172.17.0.4:5555".to_string(),
        "172.17.0.1:7654".to_string(),
    ];
    assert_eq!(expected_result, to_string_vector(&mut names))
}
/*
    to_string_vector: &mut Vec<SocketAddr> -> Vec<String>
    REQUIRES: node_names not empty
    ENSURES: returns a vector of the fulladdresses stored in node_names,
    otherwise return an empty vector
*/
fn to_string_vector(node_names: &mut Vec<SocketAddr>) -> Vec<String> {
    trace!(
        "Attempting to pull the full addresses in socket list {:?} into a vector",
        node_names
    );
    let mut names = Vec::new();
    for n in node_names {
        names.push(n.to_string());
    }
    trace!("Success! returning {:?} from socket list", names);
    names
}

#[test]
fn test_make_nodemap() {
    let mut expected_result = HashMap::new();
    expected_result.insert(
        "172.17.0.2:5671".to_string(),
        Node::new("172.17.0.2:5671", 5),
    );
    expected_result.insert(
        "172.17.0.3:1234".to_string(),
        Node::new("172.17.0.3:1234", 5),
    );
    expected_result.insert(
        "172.17.0.4:5555".to_string(),
        Node::new("172.17.0.4:5555", 5),
    );
    let mut names = vec![
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 17, 0, 2)),
            5671,
        ),
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 17, 0, 3)),
            1234,
        ),
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 17, 0, 4)),
            5555,
        ),
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 17, 0, 1)),
            7654,
        ),
    ];
    let my_full_address = "172.17.0.1:7654";
    let map = make_nodemap(&mut names, my_full_address, 5);
    println!("Expected Map {:?}", expected_result);
    println!("My Map: {:?}", map);
    assert_eq!(expected_result, map);
}
/*
    make_nodemap: &Vec<SocketAddr> * &str * i64 -> Hashmap<String, Node>
    REQUIRES: node_names not empty, full_address a proper ip:port address, lifetime the
    number of ticks before a node is "dead"
    ENSURES: returns a HashMap of Nodes referenced by the ip:port address
*/
fn make_nodemap(
    node_names: &[SocketAddr],
    full_address: &str,
    lifetime: i64,
) -> HashMap<String, Node> {
    debug!{"Initialize hashmap of nodes with lifetime {} from socket list {:?} not including {}", lifetime, node_names, full_address};
    let mut nodes = HashMap::new();
    for node_ip in node_names {
        if node_ip.to_string() != full_address {
            debug!("node ip addresses and port: {:?}", node_ip);
            let mut temp_node = Node::new(&node_ip.to_string(), lifetime);
            debug!("Node successfully created : {:?}", &temp_node);
            nodes.insert(node_ip.to_string(), temp_node);
        }
    }
    nodes
}

#[test]
fn test_add_node_to_map() {
    let mut map = HashMap::new();
    map.insert(
        "172.17.0.2:5671".to_string(),
        Node::new("172.17.0.2:5671", 5),
    );
    map.insert(
        "172.17.0.3:1234".to_string(),
        Node::new("172.17.0.3:1234", 5),
    );
    map.insert(
        "172.17.0.4:5555".to_string(),
        Node::new("172.17.0.4:5555", 5),
    );

    let mut expected_result = HashMap::new();
    expected_result.insert(
        "172.17.0.2:5671".to_string(),
        Node::new("172.17.0.2:5671", 5),
    );
    expected_result.insert(
        "172.17.0.3:1234".to_string(),
        Node::new("172.17.0.3:1234", 5),
    );
    expected_result.insert(
        "172.17.0.4:5555".to_string(),
        Node::new("172.17.0.4:5555", 5),
    );

    add_node_to_map(&mut map, "172.17.0.3:1234", 5, false);
    assert_eq!(expected_result, map);

    let mut expected_result = HashMap::new();
    expected_result.insert(
        "172.17.0.2:5671".to_string(),
        Node::new("172.17.0.2:5671", 5),
    );
    expected_result.insert(
        "172.17.0.3:1234".to_string(),
        Node::new("172.17.0.3:1234", 5),
    );
    expected_result.insert(
        "172.17.0.4:5555".to_string(),
        Node::new("172.17.0.4:5555", 5),
    );
    expected_result.insert(
        "172.17.0.1:7654".to_string(),
        Node::new("172.17.0.1:7654", 5),
    );

    add_node_to_map(&mut map, "172.17.0.1:7654", 5, false);
    assert_eq!(expected_result, map);
}
/*
    add_node_to_map: &mut Hashmap<String, Node> * &str * i64 * bool -> null
    REQUIRES: full_address is a properly formatted full address in the form ip:port. Lifetime > 0
    ENSURES: If a node with the name of full_address is not already in the hashmap,
    add a node with the name full_address, a lifetime of input lifetime, and a had_heartbeat 
    value of heartbeat to the hashmap nodes. 
*/
fn add_node_to_map(
    nodes: &mut HashMap<String, Node>,
    full_address: &str,
    lifetime: i64,
    heartbeat: bool,
) {
    trace!("Adding node to map");
    if !nodes.contains_key(full_address) {
        debug!("node ip addresses and port to add: {}", full_address);
        let temp_node = Node::node_new(full_address, lifetime, lifetime, heartbeat);
        debug!("Node successfully created : {:?}", &temp_node);
        nodes.insert(full_address.to_string(), temp_node);
    }
}
/**
 * add_node_to_cluster: &mut Vec<SocketAddr> * &str * &mut HashMap<String,Node> * i64 * vool * &mut Socket -> null
 * REQUIRES: makes a new node given that the node names does not previously exist, and adds itself to both the
 * node_Names and the nodes, and connects the node to given.  Otherwise it does nothing.
 */
fn add_node_to_cluster(
    node_names: &mut Vec<SocketAddr>,
    full_address: &str,
    nodes: &mut HashMap<String, Node>,
    lifetime: i64,
    heartbeat: bool,
    router: &mut Socket,
) {
    if !nodenames_contain_full_address(&full_address.to_string(), node_names) {
        debug!("Node names before adding {:?}", node_names);
        debug!("Node Map before adding {:?}", nodes);
        match add_node_to_list(&full_address, node_names) {
            Ok(t) => t,
            Err(e) => error!(
                "Unable to parse socket address, should be in the form ip:port:{:?}",
                e
            ),
        };
        add_node_to_map(nodes, &full_address, lifetime, heartbeat);
        match connect_node(&full_address, router) {
            Ok(t) => t,
            Err(e) => error!("Unable to connect to the node at ip address: {}", e),
        };
        debug!("Node names after adding {:?}", node_names);
        debug!("Node Map after adding {:?}", nodes);
    }
}

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
    let current_port = get_port_from_fulladdr(full_address)?;
    router.bind(&format!("tcp://*:{}", current_port))?;
    debug!("router bound to port {}", current_port);
    Ok(router)
}

/**
 * connect_node: &str * &mut Socket -> ForkliftResult<()>
 * REQUIRES: full_address is properly formatted as ip:port, router is a valid Socket
 * ENSURES: connects router to the address of full_address, output
 * error otherwise
 */
fn connect_node(full_address: &str, router: &mut Socket) -> ForkliftResult<()> {
    debug!("Try to connect router to {}", full_address);
    let tcp: String = format!("tcp://{}", full_address);
    router.connect(&tcp)?;
    Ok(())
}

/**
 * send_getlist: &PollRequest * &mut u64 * &str * router &mut Socket * u64 -> ForkliftResult<()>
 * REQUIRES: &PollRequest a value file descriptor, pulse a valid Pulse, name a properly formatter
 * full_addr in the form of ip:port, router a valid socket
 * ENSURES: sends a GETLIST to the address of name 
 */
fn send_getlist(
    request: &PollRequest,
    pulse: &mut Pulse,
    name: &str,
    router: &mut Socket,
) -> ForkliftResult<()> {
    let beat = match pulse.beat() {
        Ok(t) => t,
        Err(e) => {
            error!("Time went backwards! Abort! {}", e);
            panic!("Time went backwards! Abort! {}", e)
        }
    };
    if request.get_fds()[0].can_write() && beat {
        debug!("Send a GETLIST to {}", name);
        let message = message::create_message(MessageType::GETLIST, &[name.to_string()]);
        match router.nb_write(message.as_slice()) {
            Ok(..) => debug!("GETLIST sent to {}", name),
            Err(Error::TryAgain) => error!("Receiver not ready, message can't be sent"),
            Err(..) => error!("Failed to write to socket!"),
        };
    }
    Ok(())
}

/**
 * send_nodelist: &mut Vec<SocketAddr> * &[String] * &mut Hashmap<String, Node> * i64 * &mut Socket -> null
 * REQUIRES: node_names non-empty, msg_body ) should be non-empty, with the first and only item in the message body 
 * from the GETLIST recieved message body (containing the address of the node asking for the NODELIST).  nodes non-empty,
 * lifetime > 0, router a valid connected socket
 * ENSURES: The router sends a NODELIST to the sender of a GETLIST request, otherwise it does nothing if the message body
 * of the GETLIST request is empty.  
 */
fn send_nodelist(
    node_names: &mut Vec<SocketAddr>,
    msg_body: &[String],
    nodes: &mut HashMap<String, Node>,
    lifetime: i64,
    router: &mut Socket,
) {
    let address_names = to_string_vector(node_names);
    let buffer = message::create_message(MessageType::NODELIST, &address_names);

    if !msg_body.is_empty() {
        let sent_address = &msg_body[0];
        add_node_to_cluster(node_names, &sent_address, nodes, lifetime, true, router);
        debug!("Send a NODELIST to {}", sent_address);
        match router.nb_write(buffer.as_slice()) {
            Ok(_) => debug!("NODELIST sent to {}!", sent_address),
            Err(Error::TryAgain) => error!("Receiver not ready, message can't be sen't"),
            Err(err) => error!("Problem while writing: {}", err),
        };
    }
}

/**
 * send_heartbeat: &str * &mut Socket -> null
 * REQUIRES: name is your full_address in the format ip:port, router a valid Socket
 * ENSURES: sends a HEARTBEAT message to all connected nodes
 */
fn send_heartbeat(name: &str, router: &mut Socket) {
    debug!("Send a HEARTBEAT!");
    let buffer = vec![name.to_string()];
    let msg = message::create_message(MessageType::HEARTBEAT, &buffer);
    match router.nb_write(msg.as_slice()) {
        Ok(_) => debug!("HEARTBEAT sent!"),
        Err(Error::TryAgain) => {
            error!("Receiver not ready, message can't be sent for the moment ...")
        }

        Err(err) => error!("Problem while writing: {}", err),
    };
}

#[test]
fn test_tickdown_nodes() {
    let mut test_nodes: HashMap<String, Node> = HashMap::new();
    test_nodes.insert(
        "192.168.1.1:5250".to_string(),
        Node::new("192.168.1.1:5250", 5),
    );
    let node_list = vec!["192.168.1.1:5250".to_string()];

    tickdown_nodes(&mut test_nodes, &node_list);

    assert_eq!(test_nodes.get("192.168.1.1:5250").unwrap().liveness, 4);
}

/**
 * tickdown_nodes: &mut HashMap<String, Node> * &[String] -> null
 * REQUIRES: nodes not empty, node_names not empty
 * ENSURES: for all nodes that have not sent a HEARTBEAT message to you within
 * a second, tickdown their liveness.  For all nodes that HAVE sent you a
 * HEARTBEAT message, reset their has_heartbeat value to false
 */
fn tickdown_nodes(nodes: &mut HashMap<String, Node>, node_names: &[String]) {
    trace!("Tickdown and reset nodes");
    for i in node_names {
        nodes.entry(i.to_string()).and_modify(|n| {
            if !n.has_heartbeat {
                n.tickdown();
            } else {
                n.has_heartbeat = false;
                debug!("HEARTBEAT was heard for node {:?}", n);
            }
        });
    }
}

/**
 * send_and_tickdown: &PollRequest * &mut u64 * &str * &mut Socket * u64 * &mut HashMap<String, Node> * &mut Vec<SocketAddr> -> ForkliftRequest<()>
 * REQUIRES: request is a valid vector of PollRequests, pulse a valid Pulse object
 * name is your full address in the form ip:port, router a valid Socket,
 * nodes not empty, node_names not empty
 * ENSURES: returns Ok(()) if successfully sending a heartbeat to connected nodes and ticking down,
 * otherwise return Err
 */
fn send_and_tickdown(
    request: &PollRequest,
    pulse: &mut Pulse,
    name: &str,
    router: &mut Socket,
    nodes: &mut HashMap<String, Node>,
    node_names: &mut Vec<SocketAddr>,
) -> ForkliftResult<()> {
    if request.get_fds()[0].can_write() {
        let beat = match pulse.beat() {
            Ok(can_beat) => can_beat,
            Err(e) => {
                error!("Time went backwards! Abort! {}", e);
                panic!("Time went backwards! Abort! {}", e)
            }
        };

        if beat {
            send_heartbeat(name, router);
            let address_names = to_string_vector(node_names);
            tickdown_nodes(nodes, &address_names);
        }
    }
    Ok(())
}

/**
 * read_message_to_u8: &mut Socket -> Vec<u8>
 * REQUIRES: router a valid working socket
 * ENSURES: returns the next message queued to the router as a Vec<u8>
 */
fn read_message_to_u8(router: &mut Socket) -> Vec<u8> {
    let mut buffer = Vec::new();
    match router.nb_read_to_end(&mut buffer) {
        Ok(_) => debug!("Read message {} bytes!", buffer.len()),
        Err(Error::TryAgain) => error!("Nothing to be read"),
        Err(err) => error!("Problem while reading: {}", err),
    };
    buffer
}

/*
    NOTE: This test can only check if the function can successfully parse a &[u8] message 
    into the hashmap and list of nodes.  It cannot check if the router successfully connects to the 
    message input nodes
*/
#[test]
fn test_parse_nodelist_message() {
    let msg: Vec<u8> = vec![
        12, 0, 0, 0, 8, 0, 12, 0, 7, 0, 8, 0, 8, 0, 0, 0, 0, 0, 0, 1, 4, 0, 0, 0, 3, 0, 0, 0, 12,
        0, 0, 0, 32, 0, 0, 0, 52, 0, 0, 0, 16, 0, 0, 0, 49, 57, 50, 46, 49, 54, 56, 46, 49, 46, 49,
        58, 53, 50, 53, 48, 0, 0, 0, 0, 16, 0, 0, 0, 49, 55, 50, 46, 49, 49, 49, 46, 50, 46, 50,
        58, 53, 53, 53, 53, 0, 0, 0, 0, 14, 0, 0, 0, 55, 50, 46, 49, 50, 46, 56, 46, 56, 58, 56,
        48, 56, 48, 0, 0,
    ];
    let mut names = vec![
        "123.45.67.89:9999".parse::<SocketAddr>().unwrap(),
        "231.54.76.98:1111".parse::<SocketAddr>().unwrap(),
    ];

    let mut testnames = vec![
        "123.45.67.89:9999".parse::<SocketAddr>().unwrap(),
        "231.54.76.98:1111".parse::<SocketAddr>().unwrap(),
    ];

    let mut nodes: HashMap<String, Node> = HashMap::new();
    nodes.insert(
        "222.33.44.55:5555".to_string(),
        Node::new("222.33.44.55:5555", 7),
    );
    nodes.insert(
        "66.77.88.99:8080".to_string(),
        Node::new("66.77.88.99:8080", 5),
    );

    let mut cmpnodes: HashMap<String, Node> = HashMap::new();
    cmpnodes.insert(
        "222.33.44.55:5555".to_string(),
        Node::new("222.33.44.55:5555", 7),
    );
    cmpnodes.insert(
        "66.77.88.99:8080".to_string(),
        Node::new("66.77.88.99:8080", 5),
    );

    let mut router = Socket::new(Protocol::Bus).unwrap();
    let mut has_nodelist = true;

    parse_nodelist_message(
        &msg,
        &mut names,
        &mut nodes,
        5,
        &mut router,
        &mut has_nodelist,
    );

    assert_eq!(names, testnames);
    assert_eq!(nodes, cmpnodes);
    assert_eq!(has_nodelist, true);

    testnames.push("192.168.1.1:5250".parse::<SocketAddr>().unwrap());
    testnames.push("172.111.2.2:5555".parse::<SocketAddr>().unwrap());
    testnames.push("72.12.8.8:8080".parse::<SocketAddr>().unwrap());

    cmpnodes.insert(
        "192.168.1.1:5250".to_string(),
        Node::new("192.168.1.1:5250", 5),
    );
    cmpnodes.insert(
        "172.111.2.2:5555".to_string(),
        Node::new("172.111.2.2:5555", 5),
    );
    cmpnodes.insert("72.12.8.8:8080".to_string(), Node::new("72.12.8.8:8080", 5));

    has_nodelist = false;
    parse_nodelist_message(
        &msg,
        &mut names,
        &mut nodes,
        5,
        &mut router,
        &mut has_nodelist,
    );

    assert_eq!(names, testnames);
    assert_eq!(nodes, cmpnodes);
    assert_eq!(has_nodelist, true);

    has_nodelist = false;
    parse_nodelist_message(
        &msg,
        &mut names,
        &mut nodes,
        5,
        &mut router,
        &mut has_nodelist,
    );

    assert_eq!(names, testnames);
    assert_eq!(nodes, cmpnodes);
    assert_eq!(has_nodelist, true);
}

/**
 * parse_nodelist_message: &[u8] * &mut Vec<SocketAddr> * &mut HashMap<String, Node> * i64 * &mut Socket, &mut bool -> null
 * REQUIRES: buf a message read from the socket, node_names not empty, nodes not empty, liveness the lifetime value of a new node > 0,
 * router a working, valid Socket, has_nodelist is false
 * ENSURES: parses a NODELIST message into a node_list and creates/adds the nodes received to the cluster
 */
fn parse_nodelist_message(
    buf: &[u8],
    node_names: &mut Vec<SocketAddr>,
    nodes: &mut HashMap<String, Node>,
    liveness: i64,
    router: &mut Socket,
    has_nodelist: &mut bool,
) {
    if !*has_nodelist {
        debug!("Parse the NODELIST!");
        let list = match message::read_message(buf) {
            Some(t) => t,
            None => {
                error!("NODELIST message is empty");
                vec![]
            }
        };
        for l in &list {
            add_node_to_cluster(node_names, &l, nodes, liveness, false, router)
        }
        if !list.is_empty() {
            *has_nodelist = true;
        }
    }
}

/**
 * NOTE: We can't test if the socket actually binds to a new connection if a new heartbeat node
 * is heart from, but we can test if it properly changes the Node liveness, and adds to the
 * map and name list.  
 */
#[test]
fn test_heartbeat_heard() {
    let mut nodes: HashMap<String, Node> = HashMap::new();
    nodes.insert(
        "172.77.123.11:5555".to_string(),
        Node::node_new("172.77.123.11:555", 5, 3, false),
    );
    nodes.insert(
        "123.45.67.89:9999".to_string(),
        Node::node_new("123.45.67.89:9999", 5, 2, false),
    );
    nodes.insert(
        "192.168.1.1:5250".to_string(),
        Node::node_new("192.168.1.1:5250", 5, 1, false),
    );

    let msg = vec!["192.168.1.1:5250".to_string()];
    let mut router = Socket::new(Protocol::Bus).unwrap();

    assert_eq!(3, nodes["172.77.123.11:5555"].liveness);
    assert_eq!(2, nodes["123.45.67.89:9999"].liveness);
    assert_eq!(1, nodes["192.168.1.1:5250"].liveness);

    let mut names = vec![
        "172.77.123.11:5555".parse::<SocketAddr>().unwrap(),
        "123.45.67.89:9999".parse::<SocketAddr>().unwrap(),
        "192.168.1.1:5250".parse::<SocketAddr>().unwrap(),
    ];
    let mut testnames = vec![
        "172.77.123.11:5555".parse::<SocketAddr>().unwrap(),
        "123.45.67.89:9999".parse::<SocketAddr>().unwrap(),
        "192.168.1.1:5250".parse::<SocketAddr>().unwrap(),
    ];
    heartbeat_heard(&msg, &mut names, &mut nodes, 5, &mut router);

    assert_eq!(3, nodes["172.77.123.11:5555"].liveness);
    assert_eq!(2, nodes["123.45.67.89:9999"].liveness);
    assert_eq!(5, nodes["192.168.1.1:5250"].liveness);
    assert_eq!(testnames, names);

    let msg = vec!["123.23.45.45:5656".to_string()];
    testnames.push("123.23.45.45:5656".parse::<SocketAddr>().unwrap());
    heartbeat_heard(&msg, &mut names, &mut nodes, 5, &mut router);
    assert_eq!(3, nodes["172.77.123.11:5555"].liveness);
    assert_eq!(2, nodes["123.45.67.89:9999"].liveness);
    assert_eq!(5, nodes["192.168.1.1:5250"].liveness);
    assert_eq!(5, nodes["123.23.45.45:5656"].liveness);
    assert_eq!(testnames, names);
}

/**
 * heartbeat_heard: &[String] * &mut Vec<SocketAddr> * &mut HashMap<String, Node> * i64 * &mut Socket &str -> null
 * REQUIRES: msg_body not empty, node_names not empty, nodes not empty, liveness the lifetime of a node, router a
 * valid Socket, full_address a properly formatted ip:port string
 * ENSURES: updates the hashmap to either: add a new node if the heartbeart came from a new node,
 * or updates the liveness of the node the heartbeat came from
 */
fn heartbeat_heard(
    msg_body: &[String],
    node_names: &mut Vec<SocketAddr>,
    nodes: &mut HashMap<String, Node>,
    liveness: i64,
    router: &mut Socket,
) {
    if !msg_body.is_empty() {
        let sent_address = &msg_body[0];
        add_node_to_cluster(node_names, &sent_address, nodes, liveness, true, router);
        nodes
            .entry(sent_address.to_string())
            .and_modify(|n| n.heartbeat());
    }
}

/**
 * read_and_heartbeat: &PollRequest * &mut Socket * &mut Vec<SocketAddr> * &mut HashMap<String, Node> * i64 * &mut bool * &mut u64 * &str * u64 -> null
 * REQUIRES: request not empty, router is connected, node_names not empty, nodes not empty, liveness > 0, pulse a valid Pulse object,
 * full_address is properly formatted as ip:port,
 * ENSURES: reads incoming messages and sends out heartbeats every interval milliseconds.  
 */
fn read_and_heartbeat(
    request: &PollRequest,
    router: &mut Socket,
    node_names: &mut Vec<SocketAddr>,
    nodes: &mut HashMap<String, Node>,
    liveness: i64,
    has_nodelist: &mut bool,
    pulse: &mut Pulse,
    full_address: &str,
) {
    if request.get_fds()[0].can_read() {
        //check message type
        let msg = read_message_to_u8(router);
        let msgtype = message::get_message_type(&msg);
        let msg_body = match message::read_message(&msg) {
            Some(t) => t,
            None => {
                error!("Message body is empty. Ignore the message");
                vec![]
            }
        };
        match msgtype {
            MessageType::NODELIST => {
                debug!("Can read message of type NODELIST");
                parse_nodelist_message(&msg, node_names, nodes, liveness, router, has_nodelist)
            }
            MessageType::GETLIST => {
                debug!("Can read message of type GETLIST");
                send_nodelist(node_names, &msg_body, nodes, liveness, router)
            }
            MessageType::HEARTBEAT => {
                debug!("Can read message of type HEARTBEAT");
                heartbeat_heard(&msg_body, node_names, nodes, liveness, router);
                if !*has_nodelist {
                    match send_getlist(request, pulse, full_address, router) {
                        Ok(t) => t,
                        Err(e) => {
                            error!("Time ran backwards!  Abort! {}", e);
                            panic!("Time ran backwards! Abort! {}", e)
                        }
                    };
                }
            }
        }
    }
}
/*
    if node_joined has been flagged, then we need to connect the node to the graph. 
    This is done by sending a GETLIST signal to the node that we are connected to
    every second until we get a NODELIST back. 
    Poll THIS machine's node
        Pollin using timeout of pulse interval
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
fn heartbeat_loop(
    router: &mut Socket,
    has_nodelist: &mut bool,
    pulse: &mut Pulse,
    full_address: &str,
    node_names: &mut Vec<SocketAddr>,
    nodes: &mut HashMap<String, Node>,
    liveness: i64,
) -> ForkliftResult<()> {
    loop {
        std::thread::sleep(std::time::Duration::from_millis(10));
        let mut items: Vec<PollFd> = vec![router.new_pollfd(PollInOut::InOut)];
        let mut request = PollRequest::new(&mut items);
        trace!("Attempting to poll the socket");
        Socket::poll(&mut request, pulse.interval as isize)?;

        if !*has_nodelist {
            match send_getlist(&request, pulse, full_address, router) {
                Ok(t) => t,
                Err(e) => {
                    error!("Time ran backwards!  Abort! {}", e);
                    panic!("Time ran backwards! Abort! {}", e)
                }
            };
        }

        read_and_heartbeat(
            &request,
            router,
            node_names,
            nodes,
            liveness,
            has_nodelist,
            pulse,
            full_address,
        );

        match send_and_tickdown(&request, pulse, full_address, router, nodes, node_names) {
            Ok(t) => t,
            Err(e) => {
                error!("Time ran backwards!  Abort! {}", e);
                panic!("Time ran backwards! Abort! {}", e)
            }
        };
    }
    //Ok(())
}

fn init_connect(node_names: &mut Vec<SocketAddr>, full_address: &str, router: &mut Socket) {
    trace!("Initializing connection...");
    for node_ip in node_names {
        if node_ip.to_string() != full_address {
            trace!("Attempting to connect to {}", node_ip);
            match connect_node(&node_ip.to_string(), router) {
                Ok(t) => t,
                Err(e) => error!(
                    "Error: {} Unable to connect to the node at ip address: {}",
                    e, full_address
                ),
            };
        }
    }
}

fn heartbeat(matches: &clap::ArgMatches) -> ForkliftResult<()> {
    //Variables that don't depend on command line args
    let liveness = 5; //The amount of times we can tick down before assuming death
    let interval = 1000; //set heartbeat interval in msecs
    trace!("Initializing pulse");
    let mut pulse = match Pulse::new(interval) {
        Ok(p) => p,
        Err(e) => {
            error!("System Time went backwards! Abort! {}", e);
            panic!("System Time went backwards! Abort! {}", e)
        }
    };
    let mut has_nodelist = false;
    let joined = match matches.values_of("join") {
        None => vec![],
        Some(t) => t.collect(),
    };

    let filename = match matches.value_of("namelist") {
        None => "",
        Some(t) => {
            has_nodelist = true;
            t
        }
    };
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
    let mut node_names: Vec<SocketAddr> = vec![];
    if joined.len() == 2 {
        //NOTE: when join is called, only TWO arguments are passed in,
        //so Joined.get(1) and joined.get(0) should both work.  If it doesn't,
        //Well, there's a HUGE problem 'cause then matches didn't work or something.
        match add_node_to_list(
            match joined.get(1) {
                Some(addr) => addr,
                None => {
                    error!("Join flag did not work, second argument does not exist");
                    ""
                }
            },
            &mut node_names,
        ) {
            Ok(t) => t,
            Err(e) => error!(
                "Node Join Error: Unable to parse socket address when adding name to list; should be in the form ip:port:{:?}",
                e
            ),
        };
        match add_node_to_list(
            match joined.get(0) {
                Some(addr) => addr,
                None => {
                    error!("Join flag did not work, second argument does not exist");
                    ""
                }
            },
            &mut node_names,
        ) {
            Ok(t) => t,
            Err(e) => error!(
                "Node Join Error: Unable to parse socket address when adding name; should be in the form ip:port:{:?}",
                e
            ),
        };
    } else
    //We did not flag -j (since -j requires exactly two arguments)
    {
        node_names = match init_node_names(filename).map_err(|e| {
            error!(
                "Node List Initialization Error: Unable to parse socket address when adding name; should be in the form ip:port:{:?}",
                e
            );
            ForkliftError::InvalidConfigError
        }){
            Ok(n) => n,
            Err(e) => {error!("Unable to parse the input file into a vector of SocketAddr's.  Line format should be ip:port.  Error was {}", e);
            panic!("Unable to parse the input file into a vector of SocketAddr's.  Line format should be ip:port.  Error was {}", e)
            },
        };
    }
    let full_address = match get_full_address_from_ip(&ip_address.to_string(), &mut node_names) {
        Some(a) => a,
        None => {
            error!("ip address {} not in the node_list ", ip_address);
            "".to_string()
        } //Handle this later
    };
    debug!("current full address: {:?}", full_address);

    let mut nodes = make_nodemap(&node_names, &full_address, liveness); //create mutable hashmap of nodes
    let mut router = init_router(&full_address)?; //Make the node

    //sleep for a bit to let other nodes start up
    std::thread::sleep(std::time::Duration::from_millis(10));

    init_connect(&mut node_names, &full_address, &mut router);
    heartbeat_loop(
        &mut router,
        &mut has_nodelist,
        &mut pulse,
        &full_address,
        &mut node_names,
        &mut nodes,
        liveness,
    )
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
    let path = match dirs::home_dir() {
        Some(path) => path.join("debuglog"),
        None => {
            error!("Home directory not found");
            panic!("Home Directory not found!")
        }
    };
    let path_str = path.to_string_lossy();
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
    let logfile = Path::new(matches.value_of("logfile").unwrap());
    init_logs(&logfile, level)?;
    debug!("Logs made");
    heartbeat(&matches)?;
    Ok(())
}
