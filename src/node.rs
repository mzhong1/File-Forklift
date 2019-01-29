

use crate::error::{ForkliftError, ForkliftResult};
use crate::utils;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Eq)]
pub struct Node {
    ///name of node
    pub name: SocketAddr,
    pub lifetime: u64,
    pub liveness: i64,
    pub has_heartbeat: bool,
}

impl Node {
    pub fn new(n: SocketAddr, lt: u64) -> Self {
        Node {
            name: n,
            lifetime: lt,
            liveness: 0, //Note, new Nodes start as "Dead", so when a heartbeat is heard the list of active nodes is updated
            has_heartbeat: false,
        }
    }
    pub fn node_new(n: SocketAddr, lt: u64, l: i64, h: bool) -> Self {
        Node {
            name: n,
            lifetime: lt,
            liveness: l,
            has_heartbeat: h,
        }
    }

    /**
     * ENSURES: returns true if the node was "dead" before the heartbeat was called
     */
    pub fn heartbeat(&mut self) -> bool {
        trace!(
            "Before Heartbeat: Node {}, liveness {}",
            self.name,
            self.liveness
        );
        let prev_liveness = self.liveness;
        self.liveness = self.lifetime as i64;
        self.has_heartbeat = true;
        debug!("Heartbeat Node {}, liveness {}", self.name, self.liveness);
        prev_liveness <= 0
    }
    /**
     * ENSURES: return true if the node "died" in this call to tickdown
     */
    pub fn tickdown(&mut self) -> bool {
        trace!(
            "Before Tickdown: Node {}, liveness {}",
            self.name,
            self.liveness
        );
        let prev_liveness = self.liveness;
        if self.liveness > 0 {
            self.liveness -= 1;
        }
        self.has_heartbeat = false;
        debug!("Tickdown Node {}, liveness {}", self.name, self.liveness);
        prev_liveness == 1
    }
}
impl Hash for Node {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.lifetime.hash(state);
    }
}

impl Ord for Node {
    fn cmp(&self, other: &Node) -> Ordering {
        self.liveness.cmp(&other.liveness)
    }
}

impl PartialOrd for Node {
    fn partial_cmp(&self, other: &Node) -> Option<Ordering> {
        Some(self.cmp(other))
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
        init_node_names: &Path -> ForkliftResult<NodeList>
        REQUIRES: filename is the path to a properly formatted nonempty File (each line has the ip:port of a node)
        ENSURES: returns a NodeList of ip:port addresses wrapped in ForkliftResult,
        or returns a ForkliftError (AddrParseError, since IO errors and file parsing errors
        will fail the program).
    */
    pub fn init_node_names(filename: &Path) -> ForkliftResult<Self> {
        let node_list = match utils::read_file_lines(filename) {
            Ok(f) => f,
            Err(e) => {
                error!("Filename does not Exist, ERROR: {:?}", e);
                panic!("Filename does not Exist, ERROR: {:?}", e)
            }
        };
        if node_list.is_empty() {
            error!("File is empty! {:?}", ForkliftError::InvalidConfigError);
            panic!("File is empty!");
        }
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
        Ok(NodeList {
            node_list: node_names,
        })
    }

    /**
     * init_names: Vec<String> * PathBuf -> NodeList
     * REQUIRES: joined.len() == 2 XOR filename a path to properly formatted input file
     * ENSURES: returns a NodeList with a populated Vector of SocketAddrs.
     */
    pub fn init_names(joined: Vec<String>, filename: &PathBuf) -> Self {
        let mut names = NodeList::new();
        if joined.len() == 2 {
            for name in joined {
                let socket = match name.parse::<SocketAddr>() {
                    Ok(t) => t,
                    Err(e) => {
                        error!(
                    "Node Join Error: Unable to parse socket address when adding name to list; should be in the form ip:port:{:?}",
                    e
                    );
                        panic!("Unable to parse the socket address when adding name to list; should be in the form ip:port.  Error was {}", e)
                    }
                };
                names.add_node_to_list(&socket);
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
        get_full_address: &self * &str -> String
        REQUIRES: ip a valid ip address
        ENSURES: returns SOME(ip:port) associated with the input ip address
        that is stored in node_names, otherwise return NONE
    */
    pub fn get_full_address(&self, ip: &str) -> Option<SocketAddr> {
        trace!(
            "Attempt to get full address of input ip {} from list of sockets",
            ip
        );
        for n in &self.node_list {
            trace!("current loop address is {:?}", n);
            if n.ip().to_string() == ip {
                trace!("Successfully matched ip {} to full address {:?}", ip, n);
                return Some(*n);
            }
        }
        trace!("failed to find a matching full address in for ip {}", ip);
        None
    }

    /*
        contains_full_address &str -> bool
        REQUIRES: NONE,
        ENSURES: returns true if the full address is in one of the SocketAddr elements of node_names,
        false otherwise
    */
    pub fn contains_full_address(&self, full_address: &SocketAddr) -> bool {
        self.node_list.iter().any(|n| n == full_address)
    }

    /**
     * add_node_to_list: &self * &str -> null
     * REQUIRES: NONE
     * ENSURES: adds a new node with the address of full_address to node_names, if not already
     * in the vector, else it does nothing
     */
    pub fn add_node_to_list(&mut self, full_address: &SocketAddr) {
        trace!(
            "Attempting to add address {} to list of sockets",
            full_address
        );
        if !self.contains_full_address(full_address) {
            trace!(
                "Address {} not already in list, attempting to parse to socket",
                full_address
            );
            self.node_list.push(full_address.clone());
        }
    }

    /*
        to_string_vector: &mut Vec<SocketAddr> -> Vec<String>
        REQUIRES: NONE
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
        init_nodemap: &Vec<SocketAddr> * &str * i64 -> Hashmap<String, Node>
        REQUIRES: lifetime > 0
        ENSURES: returns a HashMap of Nodes referenced by the ip:port address
    */
    pub fn init_nodemap(
        full_address: &SocketAddr,
        lifetime: u64,
        node_names: &[SocketAddr],
    ) -> Self {
        if lifetime == 0 {
            error!("Lifetime is trivial (less than or equal to zero)!");
            panic!("Lifetime is trivial (less than or equal to zero)!");
        }
        debug! {"Initialize hashmap of nodes with lifetime {} from socket list {:?} not including {}", lifetime, node_names, full_address};
        let mut nodes = NodeMap::new();
        nodes.node_map = HashMap::new();
        for node_ip in node_names {
            if node_ip != full_address {
                debug!("node ip addresses and port: {:?}", node_ip);
                let temp_node = Node::new(*node_ip, lifetime);
                debug!("Node successfully created : {:?}", &temp_node);
                nodes.node_map.insert(node_ip.to_string(), temp_node);
            }
        }
        nodes
    }

    /**
     * add_node_to_map: &slf * &str * i64 * bool
     * REQUIRES: lifetime > 0
     * ENSURES: new full_address node is added to the NodeMap
     */
    pub fn add_node_to_map(&mut self, full_address: &SocketAddr, lifetime: u64, heartbeat: bool) {
        if lifetime == 0 {
            error!("Lifetime is trivial (less than or equal to zero)!");
            panic!("Lifetime is trivial (less than or equal to zero)!");
        }
        trace!("Adding node to map");
        let temp_node = Node::node_new(*full_address, lifetime, 0, heartbeat);
        debug!("Node successfully created : {:?}", &temp_node);
        self.node_map
            .entry(full_address.to_string())
            .or_insert(temp_node);
    }
}

#[test]
fn test_heartbeat() {
    let mut n = Node::node_new(
        SocketAddr::new(
            ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(123, 45, 67, 89)),
            1111,
        ),
        5,
        3,
        false,
    );
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
    let expected_result = SocketAddr::new(
        ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 2)),
        5555,
    );
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
    assert_eq!(
        true,
        names.contains_full_address(&SocketAddr::new(
            ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 3)),
            1234,
        ))
    );
    assert_eq!(
        false,
        names.contains_full_address(&SocketAddr::new(
            ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(122, 22, 3, 4)),
            1234,
        ))
    );
}

#[test]
fn test_add_node_to_list() {
    let mut names = NodeList::new();
    let compare_names = vec![SocketAddr::new(
        ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 3)),
        1234,
    )];

    names.add_node_to_list(&SocketAddr::new(
        ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 3)),
        1234,
    ));
    assert_eq!(names.node_list, compare_names);

    names.add_node_to_list(&SocketAddr::new(
        ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(122, 22, 3, 5)),
        1234,
    ));
    assert_eq!(2, names.node_list.len());
    assert_ne!(names.node_list, compare_names);
    assert!(names.contains_full_address(&SocketAddr::new(
        ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(122, 22, 3, 5)),
        1234,
    )));
}

#[test]
fn test_to_string_vector() {
    let mut node_list = NodeList::new();
    node_list.add_node_to_list(&SocketAddr::new(
        ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 2)),
        5671,
    ));
    node_list.add_node_to_list(&SocketAddr::new(
        ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 3)),
        1234,
    ));
    node_list.add_node_to_list(&SocketAddr::new(
        ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 4)),
        5555,
    ));
    node_list.add_node_to_list(&SocketAddr::new(
        ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 1)),
        7654,
    ));
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
        Node::new(
            SocketAddr::new(
                ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 2)),
                5671,
            ),
            5,
        ),
    );
    expected_result.insert(
        "172.17.0.3:1234".to_string(),
        Node::new(
            SocketAddr::new(
                ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 3)),
                1234,
            ),
            5,
        ),
    );
    expected_result.insert(
        "172.17.0.4:5555".to_string(),
        Node::new(
            SocketAddr::new(
                ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 4)),
                5555,
            ),
            5,
        ),
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
    let my_full_address = SocketAddr::new(
        ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 1)),
        7654,
    );
    let map = NodeMap::init_nodemap(&my_full_address, 5, &mut names);
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
        Node::new(
            SocketAddr::new(
                ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 2)),
                5671,
            ),
            5,
        ),
    );
    map.node_map.insert(
        "172.17.0.3:1234".to_string(),
        Node::new(
            SocketAddr::new(
                ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 3)),
                1234,
            ),
            5,
        ),
    );
    map.node_map.insert(
        "172.17.0.4:5555".to_string(),
        Node::new(
            SocketAddr::new(
                ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 4)),
                5555,
            ),
            5,
        ),
    );

    let mut expected_result = HashMap::new();
    expected_result.insert(
        "172.17.0.2:5671".to_string(),
        Node::new(
            SocketAddr::new(
                ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 2)),
                5671,
            ),
            5,
        ),
    );
    expected_result.insert(
        "172.17.0.3:1234".to_string(),
        Node::new(
            SocketAddr::new(
                ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 3)),
                1234,
            ),
            5,
        ),
    );
    expected_result.insert(
        "172.17.0.4:5555".to_string(),
        Node::new(
            SocketAddr::new(
                ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 4)),
                5555,
            ),
            5,
        ),
    );

    map.add_node_to_map(
        &SocketAddr::new(
            ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 3)),
            1234,
        ),
        5,
        false,
    );
    assert_eq!(expected_result, map.node_map);

    let mut expected_result = HashMap::new();
    expected_result.insert(
        "172.17.0.2:5671".to_string(),
        Node::new(
            SocketAddr::new(
                ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 2)),
                5671,
            ),
            5,
        ),
    );
    expected_result.insert(
        "172.17.0.3:1234".to_string(),
        Node::new(
            SocketAddr::new(
                ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 3)),
                1234,
            ),
            5,
        ),
    );
    expected_result.insert(
        "172.17.0.4:5555".to_string(),
        Node::new(
            SocketAddr::new(
                ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 4)),
                5555,
            ),
            5,
        ),
    );
    expected_result.insert(
        "172.17.0.1:7654".to_string(),
        Node::new(
            SocketAddr::new(
                ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 1)),
                7654,
            ),
            5,
        ),
    );

    map.add_node_to_map(
        &SocketAddr::new(
            ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 1)),
            7654,
        ),
        5,
        false,
    );
    assert_eq!(expected_result, map.node_map);
}
