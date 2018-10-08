#[derive(Debug, Serialize, Deserialize)]
pub struct NodeAddress{
    pub full_address: String,
    pub ip_address: String,
    pub port: String,
}

impl NodeAddress{
    pub fn new(full_addr: &str, ip : &str, port : &str) -> Self{
        NodeAddress {
            full_address: full_addr.to_string(),
            ip_address: ip.to_string(),
            port: port.to_string(),
        }
    }

    //NOTE: Write a format checker at some point for full_address.  Otherwise splits will be problematic
    pub fn from_full_address(full_addr: &str) -> Self{
        let split_fulladdr = full_addr.split(":");
        let vec = split_fulladdr.collect::<Vec<&str>>();
        NodeAddress{
            full_address: full_addr.to_string(),
            ip_address: vec[0].to_string(),
            port: vec[1].to_string(),
        }
    }

    pub fn from_ip_and_port(ip : &str, port: &str) -> Self{
        let full_addr = format!("{}:{}", ip, port);
        NodeAddress{
            full_address: full_addr,
            ip_address: ip.to_string(),
            port: port.to_string(),
        }
    }
}