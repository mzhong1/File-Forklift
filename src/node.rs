#[derive(Debug)]
pub struct Node {
    ///name of node
    name: String,
    pub lifetime: i64,
    pub liveness: i64,
    pub has_heartbeat: bool,
}

impl Node {
    pub fn new(n: &str, lt: i64) -> Self {
        Node {
            name: n.to_string(),
            lifetime: lt,
            liveness: 0,
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
    pub fn heartbeat(&mut self){
        self.liveness = self.lifetime;
        self.has_heartbeat = true;
    }
    pub fn tickdown(&mut self){
        self.liveness = self.liveness - 1;
        self.has_heartbeat = false;
    }
}
