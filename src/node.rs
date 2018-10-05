#[derive(Debug)]
pub struct Node {
    ///name of node
    name: String,
    pub liveness: i64,
    pub has_heartbeat: bool,
}

impl Node {
    pub fn new(n: &str) -> Self {
        Node {
            name: n.to_string(),
            liveness: 0,
            has_heartbeat: false,
        }
    }
    pub fn node_new(n: &str, l: i64, h: bool) -> Self {
        Node {
            name: n.to_string(),
            liveness: l,
            has_heartbeat: h,
        }
    }
}
