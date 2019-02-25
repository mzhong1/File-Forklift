use log::*;

use crate::error::*;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;

#[derive(Debug, Clone, Eq)]
/// object storing the information of a node
pub struct Node {
    /// name of node
    pub name: SocketAddr,
    /// how long the node can live without a heartbeat
    pub lifetime: u64,
    /// how long before node dies
    pub liveness: i64,
    /// a heartbeat was heard
    pub has_heartbeat: bool,
}

impl Node {
    /// create a dead node with lifetime lifetime
    pub fn new(name: SocketAddr, lifetime: u64) -> Self {
        Node {
            name,
            lifetime,
            liveness: 0, //Note, new Nodes start as "Dead", so when a heartbeat is heard the list of active nodes is updated
            has_heartbeat: false,
        }
    }
    /// create a new node
    pub fn node_new(name: SocketAddr, lifetime: u64, liveness: i64, has_heartbeat: bool) -> Self {
        Node { name, lifetime, liveness, has_heartbeat }
    }

    /// beats the heart of a node, resetting liveness to lifetime.
    /// ENSURES: returns true if the node was "dead" before the heartbeat was called
    pub fn heartbeat(&mut self) -> bool {
        trace!("Before Heartbeat: Node {}, liveness {}", self.name, self.liveness);
        let prev_liveness = self.liveness;
        self.liveness = self.lifetime as i64;
        self.has_heartbeat = true;
        debug!("Heartbeat Node {}, liveness {}", self.name, self.liveness);
        prev_liveness <= 0
    }

    /// if a heartbeat is missed, tickdown the liveness of a node
    /// ENSURES: return true if the node "died", reaching liveness 0 in this call to tickdown
    pub fn tickdown(&mut self) -> bool {
        trace!("Before Tickdown: Node {}, liveness {}", self.name, self.liveness);
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
/// A list of node socket addresses
pub struct NodeList {
    pub node_list: Vec<SocketAddr>,
}

impl NodeList {
    /// create a new empty nodelist
    pub fn new() -> Self {
        NodeList { node_list: vec![] }
    }
    /// new_with_list
    /// create a new nodelist with a initial vector of sockets
    pub fn new_with_list(node_list: Vec<SocketAddr>) -> Self {
        NodeList { node_list }
    }

    /// get the socket address associated with the input ip address
    /// that is stored in node_names
    pub fn get_full_address(&self, ip: &str) -> Option<SocketAddr> {
        trace!("Attempt to get full address of input ip {} from list of sockets", ip);
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

    /// returns true if the socket address is in node_names,
    ///    false otherwise
    pub fn contains_address(&self, node_address: &SocketAddr) -> bool {
        self.node_list.iter().any(|n| n == node_address)
    }

    /// add a new node to node_names, else do nothing if address already in node_names
    pub fn add_node_to_list(&mut self, node_address: &SocketAddr) {
        trace!("Attempting to add address {} to list of sockets", node_address);
        if !self.contains_address(node_address) {
            trace!("Address {} not already in list, attempting to parse to socket", node_address);
            self.node_list.push(node_address.clone());
        }
    }

    /// get the socket addresses stored in node_names as a vector of strings
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
/// Hashmap of socket addresses to Nodes
pub struct NodeMap {
    pub node_map: HashMap<String, Node>,
}

impl NodeMap {
    /// create a new empty node_map
    pub fn new() -> Self {
        NodeMap { node_map: HashMap::new() }
    }

    /// create a HashMap of Nodes referenced by the socket address
    pub fn init_nodemap(
        node_address: &SocketAddr,
        lifetime: u64,
        node_names: &[SocketAddr],
    ) -> ForkliftResult<Self> {
        if lifetime == 0 {
            return Err(ForkliftError::InvalidConfigError("Lifetime is trivial".to_string()));
        }
        debug! {"Initialize hashmap of nodes with lifetime {} from socket list {:?} not including {}", lifetime, node_names, node_address};
        let mut nodes = NodeMap::new();
        nodes.node_map = HashMap::new();
        for node_ip in node_names {
            if node_ip != node_address {
                debug!("node ip addresses and port: {:?}", node_ip);
                let temp_node = Node::new(*node_ip, lifetime);
                debug!("Node successfully created : {:?}", &temp_node);
                nodes.node_map.insert(node_ip.to_string(), temp_node);
            }
        }
        Ok(nodes)
    }

    /// add a new node address to the node_map
    pub fn add_node_to_map(
        &mut self,
        node_address: &SocketAddr,
        lifetime: u64,
        heartbeat: bool,
    ) -> ForkliftResult<()> {
        if lifetime == 0 {
            return Err(ForkliftError::HeartbeatError(
                "Lifetime of added node is trivial!".to_string(),
            ));
        }
        trace!("Adding node to map");
        let temp_node = Node::node_new(*node_address, lifetime, 0, heartbeat);
        debug!("Node successfully created : {:?}", &temp_node);
        self.node_map.entry(node_address.to_string()).or_insert(temp_node);
        Ok(())
    }
}

#[test]
fn test_heartbeat() {
    let mut n = Node::node_new(
        SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(123, 45, 67, 89)), 1111),
        5,
        3,
        false,
    );
    n.heartbeat();
    assert_eq!(n.liveness, 5);
    assert_eq!(n.has_heartbeat, true);
}

#[test]
fn test_get_full_address() {
    let mut names = NodeList::new();
    names.node_list = vec![
        SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 2)), 5671),
        SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 3)), 1234),
        SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 4)), 5555),
        SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 1)), 7654),
    ];
    let expected_result =
        SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 2)), 5555);
    assert_eq!(Some(expected_result), names.get_full_address("172.17.0.4"));
    assert_eq!(None, names.get_full_address("172.17.5.4"))
}

#[test]
fn test_contains_address() {
    let mut names = NodeList::new();
    names.node_list = vec![
        SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 2)), 5671),
        SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 3)), 1234),
        SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 4)), 5555),
        SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 1)), 7654),
    ];
    assert_eq!(
        true,
        names.contains_address(&SocketAddr::new(
            ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 3)),
            1234,
        ))
    );
    assert_eq!(
        false,
        names.contains_address(&SocketAddr::new(
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
    assert!(names.contains_address(&SocketAddr::new(
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
            SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 2)), 5671),
            5,
        ),
    );
    expected_result.insert(
        "172.17.0.3:1234".to_string(),
        Node::new(
            SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 3)), 1234),
            5,
        ),
    );
    expected_result.insert(
        "172.17.0.4:5555".to_string(),
        Node::new(
            SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 4)), 5555),
            5,
        ),
    );
    let mut names = vec![
        SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 2)), 5671),
        SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 3)), 1234),
        SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 4)), 5555),
        SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 1)), 7654),
    ];
    let my_full_address =
        SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 1)), 7654);
    let map = NodeMap::init_nodemap(&my_full_address, 5, &mut names).unwrap();
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
            SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 2)), 5671),
            5,
        ),
    );
    map.node_map.insert(
        "172.17.0.3:1234".to_string(),
        Node::new(
            SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 3)), 1234),
            5,
        ),
    );
    map.node_map.insert(
        "172.17.0.4:5555".to_string(),
        Node::new(
            SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 4)), 5555),
            5,
        ),
    );

    let mut expected_result = HashMap::new();
    expected_result.insert(
        "172.17.0.2:5671".to_string(),
        Node::new(
            SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 2)), 5671),
            5,
        ),
    );
    expected_result.insert(
        "172.17.0.3:1234".to_string(),
        Node::new(
            SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 3)), 1234),
            5,
        ),
    );
    expected_result.insert(
        "172.17.0.4:5555".to_string(),
        Node::new(
            SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 4)), 5555),
            5,
        ),
    );

    map.add_node_to_map(
        &SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 3)), 1234),
        5,
        false,
    )
    .unwrap();
    assert_eq!(expected_result, map.node_map);

    let mut expected_result = HashMap::new();
    expected_result.insert(
        "172.17.0.2:5671".to_string(),
        Node::new(
            SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 2)), 5671),
            5,
        ),
    );
    expected_result.insert(
        "172.17.0.3:1234".to_string(),
        Node::new(
            SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 3)), 1234),
            5,
        ),
    );
    expected_result.insert(
        "172.17.0.4:5555".to_string(),
        Node::new(
            SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 4)), 5555),
            5,
        ),
    );
    expected_result.insert(
        "172.17.0.1:7654".to_string(),
        Node::new(
            SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 1)), 7654),
            5,
        ),
    );

    map.add_node_to_map(
        &SocketAddr::new(::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(172, 17, 0, 1)), 7654),
        5,
        false,
    )
    .unwrap();
    assert_eq!(expected_result, map.node_map);
}
