use crate::error::*;
use crate::filesystem::FileSystemType;

//use serde_aux::container_attributes::*;
use serde_derive::*;
use std::net::SocketAddr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Input {
    pub nodes: Vec<SocketAddr>,
    pub src_server: String,
    pub dest_server: String,
    pub src_share: String,
    pub dest_share: String,
    //#[serde(deserialize_with = "deserialize_struct_case_insensitive")]
    pub system: FileSystemType,
}

impl Input {
    pub fn test_new(
        nodes: Vec<SocketAddr>,
        src_server: String,
        dest_server: String,
        src_share: String,
        dest_share: String,
        system: FileSystemType,
    ) -> Self {
        Input {
            nodes,
            src_server,
            dest_server,
            src_share,
            dest_share,
            system,
        }
    }

    pub fn new(input: &str) -> ForkliftResult<Self> {
        let i: Input = serde_json::from_str(input)?;
        Ok(i)
    }
}
