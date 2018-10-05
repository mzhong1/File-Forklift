extern crate api;
extern crate local_ip;
extern crate zmq;
#[macro_use]
extern crate log;
#[macro_use]
extern crate clap;
extern crate dirs;
extern crate nanomsg;
extern crate simplelog;

use self::api::service_generated::*;
use clap::{App, Arg};
use nanomsg::{Error, PollFd, PollInOut, PollRequest, Protocol, Socket};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

mod error;
mod mess;
mod node;

use error::ForkliftResult;
use node::Node;
use simplelog::{CombinedLogger, Config, SharedLogger, TermLogger, WriteLogger};

/*
    Heartbeat protocol
    In a worker (Dealer socket):
    Calculate liveness (how many missed heartbeats before assuming death)
    wait in zmq_poll loop one sec at a time
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
    get_port: &str * Vec<String> -> String
    REQUIRES: s an ip address, nodes is not empty
    ENSURES: returns the port associated with the input ip, otherwise
    return ""
*/
fn get_port_from_list(s: &String, nodes: &mut Vec<String>) -> String {
    let mut port = "".to_string();
    for n in nodes {
        if n.contains(s) {
            let splitip = n.split(s);
            let vec = splitip.collect::<Vec<&str>>();
            port = vec[vec.len() - 1].to_string();
        }
    }
    port
}

fn get_port_from_address(ip: &String, full_address: &String) -> String {
    let splitip = full_address.split(ip);
    let vec = splitip.collect::<Vec<&str>>();
    vec[vec.len() - 1].to_string()
}

fn get_full_address(ip: &String, node_names: &mut Vec<String>) -> String {
    let mut full_address = "".to_string();
    for n in node_names {
        if n.contains(ip) {
            full_address = n.to_string();
        }
    }
    full_address
}

fn make_nodelist_from_file(filename: &str) -> ForkliftResult<Vec<String>> {
    let reader = BufReader::new(File::open(filename)?);
    let mut ret: Vec<String> = Vec::new();
    for line in reader.lines() {
        let l = line?;
        ret.push(l);
    }

    Ok(ret)
}

fn make_nodemap(
    node_names: &mut Vec<String>,
    ip_address: &String,
    liveness: i64,
) -> HashMap<String, Node> {
    //create mutable hashmapof nodes
    let mut nodes = HashMap::new();
    //fill in vectors with default values
    for node_ip in node_names {
        if node_ip != ip_address {
            debug!("node ip addresses and port: {}", node_ip);
            let mut temp_node = Node::node_new(node_ip, liveness, false);
            debug!("Node successfully created : {:?}", &temp_node);
            nodes.insert(node_ip.to_string(), temp_node);
        }
    }
    nodes
}

fn make_node(ip_address: &String, node_names: &mut Vec<String>) -> ForkliftResult<Socket> {
    let mut router = Socket::new(Protocol::Bus)?;
    let current_port = get_port_from_list(&ip_address, node_names);
    assert!(router.bind(&format!("tcp://*{}", current_port)).is_ok());
    Ok(router)
}

fn add_node_to_map(
    nodes: &mut HashMap<String, Node>,
    node_ip: &String,
    liveness: i64,
    heartbeat: bool,
) {
    if !nodes.contains_key(node_ip) {
        debug!("node ip addresses and port: {}", node_ip);
        let temp_node = Node::node_new(node_ip, liveness, heartbeat);
        debug!("Node successfully created : {:?}", &temp_node);
        nodes.insert(node_ip.to_string(), temp_node);
    }
}

fn connect_node(node_ip: &String, router: &mut Socket) {
    let mut tcp: String = "tcp://".to_owned();
    tcp.push_str(node_ip);
    assert!(router.connect(&tcp).is_ok());
}

fn send_getlist(
    request: &PollRequest,
    c_time: u64,
    heartbeat_at: &mut u64,
    name: &String,
    router: &mut Socket,
    interval: u64,
) {
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
    node_names: &mut Vec<String>,
    nodes: &mut HashMap<String, Node>,
    liveness: i64,
    router: &mut Socket,
) {
    let list = match mess::read_message(buf) {
        Some(T) => T,
        None => vec![],
    };
    for l in list {
        make_and_add_node(node_names, &l, nodes, liveness, false, router)
    }
}
fn join_node(
    node_joined: &mut bool,
    router: &mut Socket,
    interval: u64,
    heartbeat_at: &mut u64,
    full_address: &String,
    node_names: &mut Vec<String>,
    nodes: &mut HashMap<String, Node>,
    liveness: i64,
) -> ForkliftResult<()> {
    /*
        if node_joined has been flagged, then we need to connect the node to the graph. 
        This is done by sending a GETLIST signal to the node that we are connected to
        every second until we get a NODELIST back.  From the NODELIST we can unpack the
        current list of nodes in the graph.  We then connect to the nodes in the list,
        and exit the loop. 
    */
    while !*node_joined {
        std::thread::sleep(std::time::Duration::from_millis(10));
        let mut items: Vec<PollFd> = vec![router.new_pollfd(PollInOut::InOut)];
        let mut request = PollRequest::new(&mut items);
        Socket::poll(&mut request, interval as isize);

        debug!("Poll can read: {:?}", request.get_fds()[0].can_read());
        println!("Poll can read: {:?}", request.get_fds()[0].can_read());

        let c_time = current_time_in_millis(SystemTime::now())?;
        send_getlist(
            &request,
            c_time,
            heartbeat_at,
            full_address,
            router,
            interval,
        );

        if request.get_fds()[0].can_read() {
            let buf = read_message_to_u8(router);
            if mess::get_message_type(&buf) == MessageType::NODELIST {
                parse_nodelist_message(&buf, node_names, nodes, liveness, router);
                *node_joined = true;
            }
        }
    }
    Ok(())
}

fn make_and_add_node(
    node_names: &mut Vec<String>,
    sent_address: &String,
    nodes: &mut HashMap<String, Node>,
    liveness: i64,
    heartbeat: bool,
    router: &mut Socket,
) {
    if !node_names.contains(&sent_address) {
        node_names.push(sent_address.to_string());
        add_node_to_map(nodes, &sent_address, liveness, heartbeat);
        connect_node(&sent_address, router);
    }
}

fn heartbeat_heard(nodes: &mut HashMap<String, Node>, ip_address: &str, liveness: i64) {
    nodes.entry(ip_address.to_string()).and_modify(|n| {
        n.liveness = liveness;
        n.has_heartbeat = true;
    });
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
    let path = dirs::home_dir().unwrap().join("debuglog");
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

    let mut ip_address = local_ip::get().unwrap().to_string();
    let mut nodes = HashMap::new();
    let mut node_names: Vec<String> = vec![];
    if joined.len() > 0 {
        let tempvec = joined[0].split(":").collect::<Vec<&str>>();
        ip_address = tempvec[0].to_string();
        node_names.push(joined[1].to_string());
        node_names.push(joined[0].to_string());
    } else {
        node_names = make_nodelist_from_file(filename)?;
    }
    nodes = make_nodemap(&mut node_names, &ip_address, liveness); //create mutable hashmapof nodes
    debug!("current ip address, port: {}", &ip_address);
    let mut router = make_node(&ip_address, &mut node_names)?; //Make the node
    debug!("router created");
    let full_address = get_full_address(&ip_address, &mut node_names);

    //sleep for a bit to let other nodes start up
    std::thread::sleep(std::time::Duration::from_millis(10));

    //connect to addresses
    for node_ip in &mut node_names {
        if node_ip != &ip_address {
            connect_node(node_ip, &mut router);
        }
    }
    debug!("Connection to nodes initiated");

    join_node(
        &mut node_joined,
        &mut router,
        interval,
        &mut heartbeat_at,
        &full_address,
        &mut node_names,
        &mut nodes,
        liveness,
    )?;
    /*
        Poll THIS machine's node
        Pollin using timeout of heartBeat interval
        two if loops, 1st handle POLLIN
        if can_read(): 
            if GETLIST: 
                unpack message to get the sender address
                add sender to node_list + map
                send Nodelist to sender address
            if HEARTBEAT message from some socket 
            (ip address of the heartbeat sender):
                unpack message to find out sender
                if the sender is not in the list of nodes, add it to the node_list
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
    loop {
        debug!("Looped");

        std::thread::sleep(std::time::Duration::from_millis(10));
        //let mut msg: Vec<u8> = vec![];
        let mut items: Vec<PollFd> = vec![router.new_pollfd(PollInOut::InOut)];
        let mut request = PollRequest::new(&mut items);
        Socket::poll(&mut request, interval as isize);
        debug!("Poll can read: {:?}", request.get_fds()[0].can_read());

        if request.get_fds()[0].can_read() {
            //check message type
            let msg = read_message_to_u8(&mut router);
            let msgtype = mess::get_message_type(&msg);
            let msg_body = match mess::read_message(&msg) {
                Some(T) => T,
                None => vec![],
            };
            //if GETLIST message -> send list
            if msgtype == MessageType::GETLIST {
                let buffer = mess::create_message(MessageType::NODELIST, &node_names);

                if msg_body.len() > 0 {
                    let sent_address = &msg_body[0];
                    make_and_add_node(
                        &mut node_names,
                        &sent_address,
                        &mut nodes,
                        liveness,
                        true,
                        &mut router,
                    );

                    match router.nb_write(buffer.as_slice()) {
                        Ok(_) => debug!("Node List sent!"),
                        Err(Error::TryAgain) => {
                            debug!("Receiver not ready, message can't be sen't")
                        }
                        Err(err) => debug!("Problem while writing: {}", err),
                    };
                }
            }
            //if HEARTBEAT update the nodes
            if msgtype == MessageType::HEARTBEAT {
                //check if message value is in the node_list
                if msg_body.len() > 0 {
                    let sent_address = &msg_body[0];
                    make_and_add_node(
                        &mut node_names,
                        &sent_address,
                        &mut nodes,
                        liveness,
                        true,
                        &mut router,
                    );
                    heartbeat_heard(&mut nodes, &sent_address, liveness);
                }
            }
            //otherwise we ignore the message
        }

        if request.get_fds()[0].can_write() {
            let c_time = current_time_in_millis(SystemTime::now())?;
            debug!("current time in millis {}", c_time);
            debug!("heartbeat_at {}", heartbeat_at);

            println!("current time in millis {}", c_time);
            println!("heartbeat_at {}", heartbeat_at);

            if c_time > heartbeat_at {
                let buffer = vec![full_address.clone()];
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
                for i in &node_names {
                    nodes.entry(i.to_string()).and_modify(|n| {
                        if !n.has_heartbeat {
                            n.liveness = n.liveness - 1;
                        } else {
                            n.has_heartbeat = false;
                        }
                    });
                }
            }
        }
    }
    //Ok(())
}
