extern crate zmq;

use std::fs::File;
use std::io::BufReader;
use std::io::BufRead;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

//In a worker (Dealer socket):
//Calculate liveness (how many missed heartbeats before assuming death)
// wait in zmq_poll loop one sec at a time 
//if message from other worker?  router?  reset liveness
//if no message count down
//if liveness reaches zero, consider the node dead.  


fn main()
{
    //later, when we need to get node names + ip addresses
    //let filename = "nodes.txt";
    //let node_names: Vec<_> = BufReader::new(File::open(filename).expect("Cannot open file")).lines().collect::<Result<_, _>>().expect("cannot read words");

    let num_nodes = 3; //Later we'll make it so we can set the number of nodes based on the 
    //file or whatever input
    //set liveness
    let liveness = 5; //number of times we can miss a tick
    //set heartbeat interval
    let interval = 1000; //msecs
    //set heartbeat_at
    let start = SystemTime::now();
    let since_epoch = start.duration_since(UNIX_EPOCH).expect("Time went backwards whoops");
    let heartbeat_at = (since_epoch.as_secs() * 1000 + since_epoch.subsec_nanos() as u64 / 1_000_000) + interval;

    //Make two sockets, one Dealer, one Router
    //The Dealer will handle heartbeat messages sent to it
    //The Router will send OUT heartbeats
    //The Dealer will BIND to one network 
    //The Router will BIND to another
    let context = zmq::Context::new();
    let router = context.socket(zmq::ROUTER).unwrap();


    //Build a list of DEALER sockets that the ROUTER sends to from node_names
    //the DEALER sockets connect to the ip addresses of their machines
    //the ROUTER will send out heartbeat messages to these machines every second 
    //using a loop over the DEALER sockets



    //Poll THIS machines DEALER and ROUTER
    //Pollin using timeout of 
    //two if loops, handle POLLIN
    //if DEALER POLLIN => recieved heartbeat message from some socket (ip address of the heartbeat sender)
    //handle heartbeat by: 
    //unpacking message to find out sender
    //update the liveness

}