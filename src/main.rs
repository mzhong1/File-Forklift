extern crate local_ip;
extern crate zmq;
#[macro_use]
extern crate log;
extern crate clap;
extern crate dirs;
extern crate simplelog;

use clap::{App, Arg};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::BufRead;
use std::io::BufReader;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
mod node;
use node::Node;
use simplelog::{Config, WriteLogger};

fn current_time_in_millis(start: SystemTime) -> u64 {
    let since_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards whoops");
    debug!("Time since epoch {:?}", since_epoch);
    since_epoch.as_secs() * 1000 + since_epoch.subsec_nanos() as u64 / 1_000_000
}

/*Heartbeat protocol
{
    In a worker (Dealer socket):
    Calculate liveness (how many missed heartbeats before assuming death)
    wait in zmq_poll loop one sec at a time
    if message from other worker?  router?  reset liveness
    if no message count down
    if liveness reaches zero, consider the node dead.
}
*/
fn main() {
    let path = dirs::home_dir().unwrap().join("debuglog");
    let matches = App::new("Heartbeat Logs")
        .author("Michelle")
        .about("Debug logs for the heartbeat program")
        .arg(
            Arg::with_name("logfile")
                .default_value(path.to_str().unwrap())
                .help("File to log to")
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
    if !Path::new(logfile).exists() {
        File::create(logfile).expect("Creating log file failed");
    }
    WriteLogger::init(
        level,
        Config::default(),
        OpenOptions::new().append(true).open(logfile).unwrap(),
    ).unwrap();

    println!("The path of the debuglog is {}", path.to_str().unwrap());

    //later, when we need to get node names + ip addresses
    let filename = "nodes.txt";
    let node_names: Vec<_> = BufReader::new(File::open(filename).expect("Cannot open file"))
        .lines()
        .collect::<Result<_, _>>()
        .expect("cannot read words");

    //local ip address
    let ip_address = local_ip::get().unwrap().to_string();
    debug!("current ip address: {}", ip_address);

    //set liveness, number of times we can miss a tick
    let liveness = 5; //
                      //set heartbeat interval
    let interval = 1000; //msecs
                         //set heartbeat_at
    let start = SystemTime::now();
    let mut heartbeat_at = current_time_in_millis(start) + interval;
    debug!(
        "first heartbeat (current time in millis + interval) : {}",
        heartbeat_at
    );

    //create mutable hashmapof nodes
    let mut nodes = HashMap::new();
    //fill in vectors with default values
    for node_ip in &node_names {
        if node_ip != &ip_address {
            debug!("node ip addresses: {}", node_ip);
            let mut temp_node = Node::new(node_ip);
            temp_node.liveness = liveness;
            debug!("Node successfully created : {:?}", &temp_node);
            nodes.insert(node_ip.to_string(), temp_node);
        }
    }

    //Make one Router
    //The Dealer will handle heartbeat messages sent to it
    //The Router will send OUT heartbeats
    //The Router will BIND to another
    let context = zmq::Context::new();
    let router = context.socket(zmq::ROUTER).unwrap();
    assert!(router.bind("tcp://*:5671").is_ok());
    debug!("router created");

    let dealer = context.socket(zmq::DEALER).unwrap();
    //Create address to connect to
    for node_ip in &node_names {
        if node_ip != &ip_address {
            let mut tcp: String = "tcp://".to_owned();
            let end_address: &str = ":5671";
            tcp.push_str(node_ip);
            tcp.push_str(end_address);
            assert!(dealer.connect(&tcp).is_ok());
        }
    }

    debug!("dealer created");

    //Build a list of DEALER sockets that the ROUTER sends to from node_names
    //the DEALER sockets connect to the ip addresses of their machines
    //the ROUTER will send out heartbeat messages to these machines every second
    //using a loop over the DEALER sockets
    let mut temp_dealer_map = HashMap::new();
    for node_ip in &node_names {
        if node_ip != &ip_address {
            //Create address to connect to
            let mut tcp: String = "tcp://".to_owned();
            let end_address: &str = ":5671";
            tcp.push_str(&node_ip);
            tcp.push_str(end_address);

            let temp_dealer = context.socket(zmq::DEALER).unwrap();
            assert!(dealer.connect(&tcp).is_ok());
            temp_dealer_map.insert(node_ip.to_string(), temp_dealer);
            debug!("Dealer of ip {} created", node_ip);
        }
    }
    //let node_dealers = temp_dealer_map;

    //Poll THIS machine's DEALER
    //Pollin using timeout of heartBeat interval
    //two if loops, 1st handle POLLIN
    //if DEALER POLLIN => recieved heartbeat message from some socket (ip address of the heartbeat sender)
    //handle heartbeat by:
    //unpacking message to find out sender
    //update the liveness of the sender
    //update array of bools associated with nodes, set had_heartbeat[i] to true (node had heartbeat)
    //create pollitem for THIS machine's DEALER (should be mutable)
    loop {
        debug!("Looped");
        println!("Looped");

        std::thread::sleep(std::time::Duration::from_millis(10));
        let mut msg = zmq::Message::new().unwrap();
        let mut items = [dealer.as_poll_item(zmq::POLLIN)];
        zmq::poll(&mut items, interval as i64).unwrap();
        debug!("Poll: {}", items[0].get_revents() as zmq::PollEvents);
        println!("Poll: {}", items[0].get_revents() as zmq::PollEvents);

        if items[0].is_readable() {
            if dealer.recv(&mut msg, 0).is_ok() {
                let sender_ip = match msg.as_str() {
                    None => "", //Log an error and ignore the message
                    Some(t) => t,
                };
                debug!("Message {} recieved sucessfully", sender_ip);
                println!("Message {} recieved sucessfully", sender_ip);
                nodes
                    .entry(sender_ip.to_string())
                    .and_modify(|e| {
                        e.liveness = liveness;
                        e.has_heartbeat = true
                    }).or_insert(Node::new(&sender_ip));
                debug!("Node updated successfully : {:?}", nodes[sender_ip]);
                println!("Node updated successfully : {:?}", nodes[sender_ip]);
            }
        }
        let start = SystemTime::now();
        /* 
        {
            if current SystemTime (time since epoch in msec) > heartbeat_at
            send out heartbeats to list of other nodes (vector of DEALER sockets)
            loop through hashmap of bools (has_heartbeat), if false (did not recieve heartbeat within 1 sec)
            tick down liveness of associated node
            after ticking down, reset has_heartbeat values to false
            if liveness becomes 0 or less than 0, assume node is dead (handle it however necessary)
        }
        */
        debug!("current time in millis {}", current_time_in_millis(start));
        debug!("heartbeat_at {}", heartbeat_at);

        println!("current time in millis {}", current_time_in_millis(start));
        println!("heartbeat_at {}", heartbeat_at);
        update_nodes(
            start,
            &mut heartbeat_at,
            &router,
            &ip_address,
            &node_names,
            interval,
            &mut nodes,
        );
        /*
        let c_time = current_time_in_millis(start);
        if c_time > heartbeat_at {
            //update heartbeat time
            heartbeat_at = c_time + interval;
            router.send_str(&ip_address, 0).unwrap();
            for node_ip in &node_names {
                if node_ip != &ip_address {
                    if !nodes[node_ip].has_heartbeat {
                        update_nodes(&nodes, &node_ip, nodes[node_ip].liveness - 1, false);
                    /*nodes
                            .entry(node_ip.to_string())
                            .and_modify(|e| e.liveness = e.liveness - 1)
                            .or_insert(Node::new(node_ip));*/
                    } else {
                        nodes
                            .entry(node_ip.to_string())
                            .and_modify(|e| e.has_heartbeat = false)
                            .or_insert(Node::new(node_ip));
                    }
                    if nodes[node_ip].liveness <= 0 {
                        //Handle this however (we'll probably remove the node from
                        //the rendezvous hash once it's been implemented
                    }
                }
            }
        }
        */
    }
}

fn update_nodes(
    start: SystemTime,
    heartbeat_at: &mut u64,
    router: &zmq::Socket,
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
