extern crate zmq;

use std::fs::File;
use std::io::BufReader;
use std::io::BufRead;
use std::time::Duration;
fn main()
{
    //later, when we need to get node names + ip addresses
    //let filename = "nodes.txt";
    //let node_names: Vec<_> = BufReader::new(File::open(filename).expect("Cannot open file")).lines().collect::<Result<_, _>>().expect("cannot read words");

    let num_nodes = 3; //Later we'll make it so we can set the number of nodes based on the 
    //file or whatever input
    let heartbeart_duration = Duration::new(5,0);

    let context = zmq::Context::new();
    let broker = context.socket(zmq::ROUTER).unwrap();


}