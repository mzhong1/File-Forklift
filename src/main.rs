extern crate local_ip;
extern crate zmq;
#[macro_use]
extern crate log;
#[macro_use]
extern crate clap;
extern crate dirs;
extern crate nanomsg;
extern crate simplelog;

use clap::{App, Arg};
use nanomsg::{PollFd, PollInOut, PollRequest, Protocol, Socket};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::BufRead;
use std::io::BufReader;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
mod mess;
mod node;
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
fn current_time_in_millis(start: SystemTime) -> u64 {
    let since_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards whoops");
    debug!("Time since epoch {:?}", since_epoch);
    since_epoch.as_secs() * 1000 + since_epoch.subsec_nanos() as u64 / 1_000_000
}

/*
    get_port: &str * Vec<String> -> String
    REQUIRES: s an ip address, nodes is not empty
    ENSURES: returns the port associated with the input ip, otherwise
    return ""
*/
fn get_port(s: String, nodes: Vec<String>) -> String {
    let mut port = "".to_string();
    for n in nodes {
        if n.contains(&s) {
            let splitip = n.split(&s);
            let vec = splitip.collect::<Vec<&str>>();
            port = vec[vec.len() - 1].to_string();
        }
    }
    port
}

fn make_nodelist(filename: &str) -> Vec<String> {
    BufReader::new(File::open(filename).expect("Cannot open file"))
        .lines()
        .collect::<Result<_, _>>()
        .expect("cannot read words")
}

fn make_nodemap(
    node_names: &Vec<String>,
    ip_address: String,
    liveness: i64,
) -> HashMap<String, Node> {
    //create mutable hashmapof nodes
    let mut nodes = HashMap::new();
    //fill in vectors with default values
    for node_ip in node_names {
        if node_ip != &ip_address {
            debug!("node ip addresses and port: {}", node_ip);
            let mut temp_node = Node::new(node_ip);
            temp_node.liveness = liveness;
            debug!("Node successfully created : {:?}", &temp_node);
            nodes.insert(node_ip.to_string(), temp_node);
        }
    }
    nodes
}

fn make_node(ip_address: String, node_names: Vec<String>) -> Socket {
    let mut router = match Socket::new(Protocol::Bus) {
        Ok(socket) => socket,
        Err(err) => panic!("{}", err),
    };
    let current_port = get_port(ip_address, node_names);
    assert!(
        router
            .bind(&format!("tcp://*{}", current_port))
            .is_ok()
    );
    router
}

fn connect_node(node_ip: &String, router: Socket) {
    let mut tcp: String = "tcp://".to_owned();
    tcp.push_str(node_ip);
    assert!(router.connect(&tcp).is_ok());
}

fn init_logs(f: &Path, level: simplelog::LevelFilter) {
    if !f.exists() {
        File::create(f).expect("Creating log file failed");
    }
    let mut loggers: Vec<Box<SharedLogger>> = vec![];
    if let Some(term_logger) = TermLogger::new(level, Config::default()) {
        loggers.push(term_logger);
    }
    loggers.push(WriteLogger::new(level, Config::default(), File::open(f)));
    let _ = CombinedLogger::init(loggers);
    info!("Starting up");
}

/*
    main takes in two flags: 
    j: computer is a new node, not a part of the original list
    d: create debug logs
    When the 'j' flag is raised, the program takes in the arguments ip_addr:port, otherip_addr:port
    Without the 'j' flag, the program takes in a file argument of ip_addr:port 
    addresses of all nodes in the graph
*/
fn main() {
    let path = dirs::home_dir().unwrap().join("debuglog");
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
            .conflicts_with("join")
        )
        .arg(
            Arg::with_name("logfile")
                .default_value(&path.to_string_lossy())
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
            Arg::from_usage("-j, --join <YOURIP:PORT> 
            'The ip address and port of your node in the form of IP:PORT' 
            <NODEIP:PORT> 'The ip address and port of a live node to connect 
            to in the form IP:PORT'")
            .required(false)
            .help("Joins a node not in the graph to the graph"),
        ).get_matches();
    let level = match matches.occurrences_of("v") {
        0 => simplelog::LevelFilter::Info,
        1 => simplelog::LevelFilter::Debug,
        _ => simplelog::LevelFilter::Trace,
    };
    let logfile = Path::new(matches.value_of("logfile").unwrap());
    init_logs(&logfile, level);

    let joined = match matches.values_of("join") {
        None => vec![],
        Some(T) => T.collect()
    };

    let filename = match matches.value_of("namelist")
    {
        None => "",
        Some(T) => T
    }; 
    

    //Variables that don't depend on command line args
    let liveness = 5; //The amount of times we can tick down before assuming death
    let interval = 1000;     //set heartbeat interval in msecs
    let start = SystemTime::now();
    let mut heartbeat_at = current_time_in_millis(start) + interval;
    debug!(
        "first heartbeat (current time in millis + interval) : {}",
        heartbeat_at
    );

    let mut ip_address = local_ip::get().unwrap().to_string();
    let mut nodes = HashMap::new();
    let mut node_names: Vec<String> = vec![]; 
    if joined.len() > 0
    //Do setup here
    {
        let tempvec = joined[0].split(":").collect::<Vec<&str>>();
        ip_address = tempvec[0].to_string();
        node_names.push(joined[1].to_string());
        node_names.push(joined[0].to_string());
    }
    else
    {
        node_names = make_nodelist(filename);
        nodes = make_nodemap(&node_names, ip_address, liveness); //create mutable hashmapof nodes
        
    }
    debug!("current ip address, port: {}", ip_address);
    let mut router = make_node(ip_address, node_names); //Make the node
    debug!("router created");

    //sleep for a bit to let other nodes start up
    std::thread::sleep(std::time::Duration::from_millis(10));

    //connect to addresses
    for node_ip in &node_names {
        if node_ip != &ip_address {
            connect_node(node_ip, router);
        }
    }
    debug!("Connection to nodes initiated");

    /*
        Poll THIS machine's DEALER
        Pollin using timeout of heartBeat interval
        two if loops, 1st handle POLLIN
        if DEALER POLLIN => recieved heartbeat message from some socket (ip address of the heartbeat sender)
        handle heartbeat by:
        unpacking message to find out sender
        update the liveness of the sender
        update array of bools associated with nodes, set had_heartbeat[i] to true (node had heartbeat)
        create pollitem for THIS machine's DEALER (should be mutable)
    */
    loop {
        debug!("Looped");
        println!("Looped");

        std::thread::sleep(std::time::Duration::from_millis(10));
        let mut msg = zmq::Message::new().unwrap();
        let mut items: Vec<PollFd> = vec![router.new_pollfd(PollInOut::In)];
        let mut request = PollRequest::new(&mut items);
        let result = Socket::poll(&mut request, interval as isize);
        debug!("Poll can read: {:?}", request.get_fds()[0].can_read());
        println!("Poll can read: {:?}", request.get_fds()[0].can_read());

        if request.get_fds()[0].can_read() {
            //check message type
            //if OHAI message -> add node
            //if GETLIST message -> send list
            //if NODELIST message should be in other loop
            //if HEARTBEAT update the nodes
        }
        let start = SystemTime::now();
        /* 
            if current SystemTime (time since epoch in msec) > heartbeat_at
            send out heartbeats to list of other nodes (vector of DEALER sockets)
            loop through hashmap of bools (has_heartbeat), if false (did not recieve heartbeat within 1 sec)
            tick down liveness of associated node
            after ticking down, reset has_heartbeat values to false
            if liveness becomes 0 or less than 0, assume node is dead (handle it however necessary)
        */
        debug!("current time in millis {}", current_time_in_millis(start));
        debug!("heartbeat_at {}", heartbeat_at);

        println!("current time in millis {}", current_time_in_millis(start));
        println!("heartbeat_at {}", heartbeat_at);
        update_nodes(
            start,
            &mut heartbeat_at,
            router,
            &ip_address,
            &node_names,
            interval,
            &mut nodes,
        );
    }
}

fn update_nodes(
    start: SystemTime,
    heartbeat_at: &mut u64,
    router: Socket,
    ip_address: &String,
    node_names: &Vec<String>,
    interval: u64,
    nodes: &mut HashMap<String, Node>,
) -> u64 {
    let c_time = current_time_in_millis(start);
    if c_time > *heartbeat_at {
        //update heartbeat time
        debug!("Current heartbeat at: {}", heartbeat_at);
        println!("Current heartbeat at: {}", heartbeat_at);
        *heartbeat_at = c_time + interval;
        debug!("Heartbeat at updated successfully: {}", heartbeat_at);
        println!("Heartbeat at updated successfully: {}", heartbeat_at);
        router.send_str(&ip_address, 0).unwrap();

        debug!(
            "Router Events: {}",
            router.get_events().unwrap() as zmq::PollEvents
        );
        println!(
            "Router Events: {}",
            router.get_events().unwrap() as zmq::PollEvents
        );
        for node_ip in node_names {
            if node_ip != ip_address {
                if !nodes[node_ip].has_heartbeat {
                    //update_nodes(&nodes, &node_ip, nodes[node_ip].liveness - 1, false);
                    nodes
                        .entry(node_ip.to_string())
                        .and_modify(|e| e.liveness = e.liveness - 1)
                        .or_insert(Node::new(node_ip));
                    debug!("Node liveness ticked down: {:?}", nodes[node_ip]);
                    println!("Node liveness ticked down: {:?}", nodes[node_ip]);
                } else {
                    nodes
                        .entry(node_ip.to_string())
                        .and_modify(|e| e.has_heartbeat = false)
                        .or_insert(Node::new(node_ip));
                    debug!("Node has_heartbeat reset: {:?}", nodes[node_ip]);
                    println!("Node liveness ticked down: {:?}", nodes[node_ip]);
                }
                if nodes[node_ip].liveness <= 0 {
                    //Handle this however (we'll probably remove the node from
                    //the rendezvous hash once it's been implemented
                }
            }
        }
        c_time + interval
    } else {
        *heartbeat_at
    }
}
