use rendezvous_hash;

use rendezvous_hash::Node as RNode;
use rendezvous_hash::NodeHasher;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::net::IpAddr;
use std::net::SocketAddr;

#[derive(Debug, Copy, Clone, Eq)]
/// hashable node containing a Socket Address
pub struct SocketNode {
    /// Socket Address and node identifier
    id: SocketAddr,
}
impl SocketNode {
    /// create a new SocketNode
    pub fn new(id: SocketAddr) -> Self {
        SocketNode { id }
    }
    /// get the ip address
    pub fn get_ip(&self) -> IpAddr {
        self.id.ip()
    }
    /// get the port number
    pub fn get_port(&self) -> u16 {
        self.id.port()
    }
}

impl Hash for SocketNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}
impl Ord for SocketNode {
    fn cmp(&self, other: &SocketNode) -> Ordering {
        if self.id.ip() > other.id.ip() {
            Ordering::Greater
        } else if self.id.ip() < other.id.ip() {
            Ordering::Less
        } else if self.id.port() > other.id.port() {
            Ordering::Greater
        } else if self.id.port() < other.id.port() {
            Ordering::Less
        } else {
            Ordering::Equal
        }
    }
}

impl PartialOrd for SocketNode {
    fn partial_cmp(&self, other: &SocketNode) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl RNode for SocketNode {
    type NodeId = Self;
    type HashCode = u64;
    fn node_id(&self) -> &Self::NodeId {
        self
    }
    fn hash_code<H, U: Hash>(&self, hasher: &H, item: &U) -> Self::HashCode
    where
        H: NodeHasher<Self::NodeId>,
    {
        hasher.hash(self, item)
    }
}

impl PartialEq for SocketNode {
    fn eq(&self, other: &SocketNode) -> bool {
        self.id == other.id
    }
}

/// enum determining how the active node list is changing
pub enum ChangeType {
    /// Add a node to the active node list
    AddNode,
    /// remove a node from the active node list
    RemNode,
}

/// wrapper to hold change type and node it effects
pub struct ChangeList {
    /// kind of change to make to active node list
    pub change_type: ChangeType,
    /// node to add/remove from active node list
    pub socket_node: SocketNode,
}

impl ChangeList {
    /// create a new ChangeList
    pub fn new(change_type: ChangeType, socket_node: SocketNode) -> Self {
        ChangeList { change_type, socket_node }
    }
}
