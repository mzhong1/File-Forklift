extern crate local_ip;
extern crate zmq;

use std::collections::HashMap;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::time::{SystemTime, UNIX_EPOCH};
mod node;
use node::Node;
//Heartbeat protocol
//In a worker (Dealer socket):
//Calculate liveness (how many missed heartbeats before assuming death)
// wait in zmq_poll loop one sec at a time
//if message from other worker?  router?  reset liveness
//if no message count down
//if liveness reaches zero, consider the node dead.
fn main() {
    //later, when we need to get node names + ip addresses
    let filename = "nodes.txt";
    let node_names: Vec<_> = BufReader::new(File::open(filename).expect("Cannot open file"))
        .lines()
        .collect::<Result<_, _>>()
        .expect("cannot read words");

    //local ip address
    let ip_address = local_ip::get().unwrap().to_string();
    //file or whatever input
    //set liveness
    let liveness = 5; //number of times we can miss a tick
                      //set heartbeat interval
    let interval = 1000; //msecs
                         //set heartbeat_at
    let start = SystemTime::now();
    let since_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards whoops");
    let mut heartbeat_at =
        (since_epoch.as_secs() * 1000 + since_epoch.subsec_nanos() as u64 / 1_000_000) + interval;

    //create mutable hashmapof nodes
    let mut nodes = HashMap::new();
    //fill in vectors with default values
    for node_ip in &node_names {
        if node_ip != &ip_address {
            let mut temp_node = Node::new(node_ip);
            temp_node.liveness = liveness;
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
    let dealer = context.socket(zmq::DEALER).unwrap();
    assert!(dealer.bind("tcp://*:5671").is_ok());

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
        let mut msg = zmq::Message::new().unwrap();
        let mut items = [dealer.as_poll_item(zmq::POLLIN)];
        zmq::poll(&mut items, interval as i64).unwrap();
        if items[0].is_readable() {
            if dealer.recv(&mut msg, 0).is_ok() {
                let sender_ip = match msg.as_str() {
                    None => "",
                    Some(t) => t,
                };
                nodes
                    .entry(sender_ip.to_string())
                    .and_modify(|e| {
                        e.liveness = liveness;
                        e.has_heartbeat = true
                    }).or_insert(Node::new(&sender_ip));
            }
        }
        //if current SystemTime (time since epoch in msec) > heartbeat_at
        //send out heartbeats to list of other nodes (vector of DEALER sockets)
        //loop through hashmap of bools (has_heartbeat), if false (did not recieve heartbeat within 1 sec)
        //tick down liveness of associated node
        //after ticking down, reset has_heartbeat values to false
        //if liveness becomes 0 or less than 0, assume node is dead (handle it however necessary)
        let since_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards whoops");
        let c_time = since_epoch.as_secs() * 1000 + since_epoch.subsec_nanos() as u64 / 1_000_000;
        if c_time > heartbeat_at {
            //update heartbeat time
            heartbeat_at = c_time + interval;
            router.send_str(&ip_address, 0).unwrap();
            for node_ip in &node_names {
                if node_ip != &ip_address {
                    if !nodes[node_ip].has_heartbeat {
                        nodes
                            .entry(node_ip.to_string())
                            .and_modify(|e| e.liveness = e.liveness - 1)
                            .or_insert(Node::new(node_ip));
                    } else {
                        nodes
                            .entry(node_ip.to_string())
                            .and_modify(|e| e.has_heartbeat = false)
                            .or_insert(Node::new(node_ip));
                    }
                    if nodes[node_ip].liveness <= 0
                    {
                        //Handle this however (we'll probably remove the node from
                        //the rendezvous hash once it's been implemented
                    }
                }
            }
        }
    }
}
