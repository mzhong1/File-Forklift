use crate::filesystem::FileSystemType;

use log::*;
use serde_derive::*;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Input {
    pub nodes: Vec<SocketAddr>,
    pub src_server: String,
    pub dest_server: String,
    /// Share should be formatted as '/sharename'
    pub src_share: String,
    /// Share should be formatted as '/sharename'
    pub dest_share: String,
    pub system: FileSystemType,
    pub debug_level: u32,
    pub num_threads: u32,
    pub workgroup: String,
    /// NFS is always "/" (unless using subdirectory),
    /// Samba smb url to root or subdirectory (if Glusterfs, MUST be subdirectory)
    /// Input "" if using 'default' path
    pub src_path: PathBuf,
    /// NFS is always "/" (unless using subdirectory),
    /// Samba smb url to root or subdirectory (if Glusterfs, MUST be subdirectory)
    /// Input "" if using 'default' path
    pub dest_path: PathBuf,
}

impl Input {
    pub fn new(input: &str) -> Self {
        match serde_json::from_str(input){
            Ok(e) => e,
            Err(e) =>{error!("Error {:?}, unable to parse config file!", e);panic!("Error {:?}, unable to parse config file!", e)}
        }
        /*if i.src_path.to_string_lossy().is_empty() {
            match i.system {
                FileSystemType::Nfs => i.src_path.push("/".to_string()),
                FileSystemType::Samba => {
                    let path = format!("smb://{}{}", i.src_server, i.src_share);
                    i.src_path.push(path)
                }
            }
        }*/
    }
}
