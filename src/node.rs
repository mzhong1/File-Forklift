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
#[test]
fn test_heartbeat() {
    let mut n = Node::node_new("123.45.67.89:1111", 5, 3, false);
    n.heartbeat();
    assert_eq!(n.liveness, 5);
    assert_eq!(n.has_heartbeat, true);
}
impl PartialEq for Node {
    fn eq(&self, other: &Node) -> bool {
        self.name == other.name && self.lifetime == other.lifetime
    }
}
