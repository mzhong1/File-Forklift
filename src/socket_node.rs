extern crate rendezvous_hash;

use rendezvous_hash::Node as RNode;
use rendezvous_hash::NodeHasher;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;

#[derive(Debug, Clone, Eq)]
pub struct SocketNode {
    id: SocketAddr,
}
impl SocketNode {
    pub fn new(s: SocketAddr) -> Self {
        SocketNode { id: s }
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

pub enum ChangeType {
    AddNode,
    RemNode,
}

pub struct ChangeList {
    pub change_type: ChangeType,
    pub socket_node: SocketNode,
}

impl ChangeList {
    pub fn new(ct: ChangeType, sn: SocketNode) -> Self {
        ChangeList {
            change_type: ct,
            socket_node: sn,
        }
    }
}