use crate::error::*;
use crate::filesystem::FileSystemType;

use serde_derive::*;
use std::net::SocketAddr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Input {
    pub nodes: Vec<SocketAddr>,
    pub src_server: String,
    pub dest_server: String,
    pub src_share: String,
    pub dest_share: String,
    pub system: FileSystemType,
    pub debug_level: u32,
    pub num_threads: u32,
    pub workgroup: String,
}

impl Input {
    pub fn new(input: &str) -> ForkliftResult<Self> {
        let i: Input = serde_json::from_str(input)?;
        Ok(i)
    }
}
