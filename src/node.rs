use error::ForkliftResult;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use utils;

#[derive(Debug, Clone)]
pub struct Node {
    ///name of node
    pub name: String,
    pub lifetime: i64,
    pub liveness: i64,
    pub has_heartbeat: bool,
}

impl Node {
    pub fn new(n: &str, lt: i64) -> Self {
        Node {
            name: n.to_string(),
            lifetime: lt,
            liveness: lt,
            has_heartbeat: false,
        }
    }
    pub fn node_new(n: &str, lt: i64, l: i64, h: bool) -> Self {
        Node {
            name: n.to_string(),
            lifetime: lt,
            liveness: l,
            has_heartbeat: h,
        }
    }

    pub fn heartbeat(&mut self) {
        trace!(
            "Before Heartbeat: Node {}, liveness {}",
            self.name,
            self.liveness
        );
        self.liveness = self.lifetime;
        self.has_heartbeat = true;
        debug!("Heartbeat Node {}, liveness {}", self.name, self.liveness);
    }
    pub fn tickdown(&mut self) {
        trace!(
            "Before Tickdown: Node {}, liveness {}",
            self.name,
            self.liveness
        );
        self.liveness -= 1;
        self.has_heartbeat = false;
        debug!("Tickdown Node {}, liveness {}", self.name, self.liveness);
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Node) -> bool {
        self.name == other.name && self.lifetime == other.lifetime
    }
}

#[derive(Debug, Clone)]
pub struct NodeList {
    pub node_list: Vec<SocketAddr>,
}

impl NodeList {
    pub fn new() -> Self {
        NodeList { node_list: vec![] }
    }

    /*
        init_node_names: &str -> ForkliftResult<Vec<String>>
        REQUIRES: filename is the name of a properly formatted File (each line has the ip:port of a node)
        ENSURES: returns the SocketAddr vector of ip:port addresses wrapped in ForkliftResult,
        or returns a ForkliftError (AddrParseError, since IO errors and file parsing errors
        will fail the program).
    */
    pub fn init_node_names(filename: &Path) -> ForkliftResult<Self> {
        let mut names = NodeList::new();
        let node_list = utils::read_file_lines(filename)?;
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
        names.node_list = node_names;
        Ok(names)
    }

    pub fn init_names(joined: Vec<String>, filename: PathBuf) -> Self {
        let mut names = NodeList::new();
        if joined.len() == 2 {
            for name in joined {
                match names.add_node_to_list(&name) {
                    Ok(t) => t,
                    Err(e) => {
                        error!(
                    "Node Join Error: Unable to parse socket address when adding name to list; should be in the form ip:port:{:?}",
                    e
                    );
                        panic!("Unable to parse the socket address when adding name to list; should be in the form ip:port.  Error was {}", e)
                    }
                }
            }
        } else
        //We did not flag -j (since -j requires exactly two arguments)
        {
            names = match NodeList::init_node_names(filename.as_path()) {
                Ok(n) => n,
                Err(e) => {
                    error!("Unable to parse the input file into a vector of SocketAddr's.  Line format should be ip:port.  Error was {}", e);
                    panic!("Unable to parse the input file into a vector of SocketAddr's.  Line format should be ip:port.  Error was {}", e)
                }
            };
        }
        names
    }
    /*
        get_full_address_from_ip: &str * &mut Vec<SocketAddr> -> String
        REQUIRES: ip a valid ip address, node_names is not empty
        ENSURES: returns SOME(ip:port) associated with the input ip address
        that is stored in node_names, otherwise return NONE
    */
    pub fn get_full_address(&self, ip: &str) -> Option<String> {
        trace!(
            "Attempt to get full address of input ip {} from list of sockets",
            ip
        );
        for n in &self.node_list {
            trace!("current loop address is {:?}", n);
            if n.ip().to_string() == ip {
                trace!("Successfully matched ip {} to full address {:?}", ip, n);
                return Some(n.to_string());
            }
        }
        trace!("failed to find a matching full address in for ip {}", ip);
        None
    }

    /*
        nodenames_contain_full_address &str * &mut Vec<SocketAddr> -> bool
        REQUIRES: full_address is the full ip:port address, node_names not empty,
        ENSURES: returns true if the full address is in one of the SocketAddr elements of node_names,
        false otherwise
    */
    pub fn contains_full_address(&self, full_address: &str) -> bool {
        self.node_list.iter().any(|n| n.to_string() == full_address)
    }

    /**
     * add_node_to_list: &str * &mut Vec<SocketAddr> -> null
     * REQUIRES: full_address is the full ip:port address, node_names not empty,
     * ENSURES: adds a new node with the address of full_address to node_names, if not already
     * in the vector, else it does nothing
     */
    pub fn add_node_to_list(&mut self, full_address: &str) -> ForkliftResult<()> {
        trace!(
            "Attempting to add address {} to list of sockets",
            full_address
        );
        if !self.contains_full_address(full_address) {
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
            self.node_list.push(temp_node);
        }
        Ok(())
    }

    /*
        to_string_vector: &mut Vec<SocketAddr> -> Vec<String>
        REQUIRES: node_names not empty
        ENSURES: returns a vector of the fulladdresses stored in node_names,
        otherwise return an empty vector
    */
    pub fn to_string_vector(&self) -> Vec<String> {
        trace!(
            "Attempting to pull the full addresses in socket list {:?} into a vector",
            self.node_list
        );
        let mut names = Vec::new();
        for n in &self.node_list {
            names.push(n.to_string());
        }
        trace!("Success! returning {:?} from socket list", names);
        names
    }
}

#[derive(Debug, Clone)]
pub struct NodeMap {
    pub node_map: HashMap<String, Node>,
}

impl NodeMap {
    pub fn new() -> Self {
        NodeMap {
            node_map: HashMap::new(),
        }
    }

    /*
        make_nodemap: &Vec<SocketAddr> * &str * i64 -> Hashmap<String, Node>
        REQUIRES: node_names not empty, full_address a proper ip:port address, lifetime the
        number of ticks before a node is "dead"
        ENSURES: returns a HashMap of Nodes referenced by the ip:port address
    */
    pub fn init_nodemap(full_address: &str, lifetime: i64, node_names: &[SocketAddr]) -> Self {
        debug!{"Initialize hashmap of nodes with lifetime {} from socket list {:?} not including {}", lifetime, node_names, full_address};
        let mut nodes = NodeMap::new();
        nodes.node_map = HashMap::new();
        for node_ip in node_names {
            if node_ip.to_string() != full_address {
                debug!("node ip addresses and port: {:?}", node_ip);
                let mut temp_node = Node::new(&node_ip.to_string(), lifetime);
                debug!("Node successfully created : {:?}", &temp_node);
                nodes.node_map.insert(node_ip.to_string(), temp_node);
            }
        }
        nodes
    }

    pub fn add_node_to_map(&mut self, full_address: &str, lifetime: i64, heartbeat: bool) {
        trace!("Adding node to map");
        if !self.node_map.contains_key(full_address) {
            debug!("node ip addresses and port to add: {}", full_address);
            let temp_node = Node::node_new(full_address, lifetime, lifetime, heartbeat);
            debug!("Node successfully created : {:?}", &temp_node);
            self.node_map.insert(full_address.to_string(), temp_node);
        }
    }
}

#[test]
fn test_heartbeat() {
    let mut n = Node::node_new("123.45.67.89:1111", 5, 3, false);
    n.heartbeat();
    assert_eq!(n.liveness, 5);
    assert_eq!(n.has_heartbeat, true);
}

#[test]
fn test_init_node_names() {
    let wrong_filename = Path::new("nodes");
    match NodeList::init_node_names(wrong_filename) //this should "break"
    {
        Ok(t) => {println!("{:?}", t); panic!("Should not go to this branch")},
        Err(e) => println!("Error {}", e),
    };

    let expected_result = vec![
        SocketAddr::new(
            ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 2)),
            5671,
        ),
        SocketAddr::new(
            ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 3)),
            1234,
        ),
        SocketAddr::new(
            ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 4)),
            5555,
        ),
        SocketAddr::new(
            ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 1)),
            7654,
        ),
    ];

    match NodeList::init_node_names(Path::new("nodes.txt")) {
        Ok(t) => {
            println!("Expected: {:?}", expected_result);
            println!("Vec: {:?}", t);
            assert_eq!(expected_result, t.node_list)
        }
        Err(e) => {
            println!("Error {}", e);
            panic!("Should not end up in this branch")
        }
    }

    //this should "break"
    match NodeList::init_node_names(Path::new("notnodes.txt")) {
        Ok(t) => {
            println!("{:?}", t);
            panic!("Should not go to this branch")
        }
        Err(e) => println!("Error {}", e),
    }
}

#[test]
fn test_get_full_address() {
    let mut names = NodeList::new();
    names.node_list = vec![
        SocketAddr::new(
            ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 2)),
            5671,
        ),
        SocketAddr::new(
            ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 3)),
            1234,
        ),
        SocketAddr::new(
            ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 4)),
            5555,
        ),
        SocketAddr::new(
            ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 1)),
            7654,
        ),
    ];
    let expected_result = "172.17.0.4:5555".to_string();
    assert_eq!(Some(expected_result), names.get_full_address("172.17.0.4"));
    assert_eq!(None, names.get_full_address("172.17.5.4"))
}

#[test]
fn test_contains_full_address() {
    let mut names = NodeList::new();
    names.node_list = vec![
        SocketAddr::new(
            ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 2)),
            5671,
        ),
        SocketAddr::new(
            ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 3)),
            1234,
        ),
        SocketAddr::new(
            ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 4)),
            5555,
        ),
        SocketAddr::new(
            ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 1)),
            7654,
        ),
    ];
    assert_eq!(true, names.contains_full_address("172.17.0.3:1234"));
    assert_eq!(false, names.contains_full_address("122.22.3.5:1234"));
}

#[test]
fn test_add_node_to_list() {
    let mut names = NodeList::new();
    let compare_names = vec![SocketAddr::new(
        ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 3)),
        1234,
    )];

    match names.add_node_to_list("172.17.0.3:1234") {
        Ok(_t) => assert_eq!(names.node_list, compare_names),
        Err(e) => {
            println!("Error {}", e);
            panic!("This branch should not have been taken!")
        }
    }
    match names.add_node_to_list("122.22.3.5:1234") {
        Ok(_t) => {
            assert_eq!(2, names.node_list.len());
            assert_ne!(names.node_list, compare_names);
            assert!(names.contains_full_address("122.22.3.5:1234"))
        }
        Err(e) => {
            println!("Error {}", e);
            panic!("This branch should not have been taken!")
        }
    }

    match names.add_node_to_list("122.22.3.4") {
        Ok(_t) => panic!("This branch should not have been taken!"),
        Err(e) => println!("Error {}", e),
    }
}

#[test]
fn test_to_string_vector() {
    let mut node_list = NodeList::new();
    node_list.add_node_to_list("172.17.0.2:5671").unwrap();
    node_list.add_node_to_list("172.17.0.3:1234").unwrap();
    node_list.add_node_to_list("172.17.0.4:5555").unwrap();
    node_list.add_node_to_list("172.17.0.1:7654").unwrap();
    let expected_result = vec![
        "172.17.0.2:5671".to_string(),
        "172.17.0.3:1234".to_string(),
        "172.17.0.4:5555".to_string(),
        "172.17.0.1:7654".to_string(),
    ];
    assert_eq!(expected_result, node_list.to_string_vector())
}

#[test]
fn test_init_nodemap() {
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
            ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 2)),
            5671,
        ),
        SocketAddr::new(
            ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 3)),
            1234,
        ),
        SocketAddr::new(
            ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 4)),
            5555,
        ),
        SocketAddr::new(
            ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 1)),
            7654,
        ),
    ];
    let my_full_address = "172.17.0.1:7654";
    let map = NodeMap::init_nodemap(my_full_address, 5, &mut names);
    println!("Expected Map {:?}", expected_result);
    println!("My Map: {:?}", map.node_map);
    assert_eq!(expected_result, map.node_map);
}

#[test]
fn test_add_node_to_map() {
    let mut map = NodeMap::new();
    map.node_map = HashMap::new();
    map.node_map.insert(
        "172.17.0.2:5671".to_string(),
        Node::new("172.17.0.2:5671", 5),
    );
    map.node_map.insert(
        "172.17.0.3:1234".to_string(),
        Node::new("172.17.0.3:1234", 5),
    );
    map.node_map.insert(
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

    map.add_node_to_map("172.17.0.3:1234", 5, false);
    assert_eq!(expected_result, map.node_map);

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

    map.add_node_to_map("172.17.0.1:7654", 5, false);
    assert_eq!(expected_result, map.node_map);
}
