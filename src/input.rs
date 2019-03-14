use crate::error::{ForkliftError, ForkliftResult};
use crate::filesystem::{DebugLevel, FileSystemType};

use serde_derive::*;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Config File Input
pub struct Input {
    /// Socket addresses of either all nodes in the cluster or this program's ip and another node
    pub nodes: Vec<SocketAddr>,
    /// Node lifetime
    #[serde(default = "default_lifetime")]
    pub lifetime: u64,
    /// server of the source share
    pub src_server: String,
    /// server of the destination share
    pub dest_server: String,
    /// source share name; should be formatted as '/sharename'
    pub src_share: String,
    /// destination share name; should be formatted as '/sharename'
    pub dest_share: String,
    /// Share type (Nfs or Samba)
    pub system: FileSystemType,
    /// The debug level of the filesystem context
    pub debug_level: DebugLevel,
    /// The number of threads used in the processing
    pub num_threads: u32,
    /// The workgroup of the user (Please default to WORKGROUP if not using Samba)
    #[serde(default = "default_workgroup")]
    pub workgroup: String,
    /// Path where source sync starts
    /// Use "/" for the root directory, and "/subdir/" for a subdirectory, etc.
    #[serde(default = "default_path")]
    pub src_path: PathBuf,
    /// Path where destination sync starts
    /// Use "/" for the root directory, and "/subdir/" for a subdirectory, etc.
    #[serde(default = "default_path")]
    pub dest_path: PathBuf,
    /// URL of database to log errors to, or NULL if not logging to database
    /// format is probably postgresql://postgres:password@ip:port
    pub database_url: Option<String>,
}
/// default workgroup helper
fn default_workgroup() -> String {
    "WORKGROUP".to_string()
}
/// default lifetime helper
fn default_lifetime() -> u64 {
    5
}
/// default source/dest path
fn default_path() -> PathBuf {
    let mut p = PathBuf::new();
    p.push("/");
    p
}
impl Input {
    //NOTE, send invalid config error when panicking
    /// create new Input object from config file
    pub fn new_input(config: &str) -> ForkliftResult<Self> {
        let mut input: Input = match serde_json::from_str(config) {
            Ok(e) => e,
            Err(e) => {
                return Err(ForkliftError::InvalidConfigError(format!(
                    "Error {:?}, unable to parse config file!",
                    e
                )));
            }
        };
        if input.src_server.is_empty() {
            return Err(ForkliftError::InvalidConfigError(
                "Error! source server not given!".to_string(),
            ));
        }
        if input.dest_server.is_empty() {
            return Err(ForkliftError::InvalidConfigError(
                "Error! destination server not given!".to_string(),
            ));
        }
        if input.src_share.is_empty() {
            return Err(ForkliftError::InvalidConfigError(
                "Error! source share not given!".to_string(),
            ));
        }
        if input.dest_share.is_empty() {
            return Err(ForkliftError::InvalidConfigError(
                "Error! destination share not given!".to_string(),
            ));
        }
        //check if shares starts with '/', exit if not
        if !input.src_share.starts_with('/') {
            return Err(ForkliftError::InvalidConfigError(
                "Source share does not start with '/'".to_string(),
            ));
        }
        if !input.dest_share.starts_with('/') {
            return Err(ForkliftError::InvalidConfigError(
                "Destination share does not start with '/'".to_string(),
            ));
        }
        match input.system {
            FileSystemType::Nfs => {
                // if the input is empty, exit
                if input.src_path.to_string_lossy().is_empty() {
                    return Err(ForkliftError::InvalidConfigError("Empty source path!".to_string()));
                }
                if input.dest_path.to_string_lossy().is_empty() {
                    return Err(ForkliftError::InvalidConfigError(
                        "Empty destination path!".to_string(),
                    ));
                }
            }
            FileSystemType::Samba => {
                input.src_path = Path::new(&format!(
                    "smb://{}{}{}",
                    input.src_server,
                    input.src_share,
                    input.src_path.to_string_lossy()
                ))
                .to_path_buf();
                println!("{:?}", input.src_path);
                input.dest_path = Path::new(&format!(
                    "smb://{}{}{}",
                    input.dest_server,
                    input.dest_share,
                    input.dest_path.to_string_lossy()
                ))
                .to_path_buf();
                println!("{:?}", input.dest_path);
            }
        }
        Ok(input)
    }
}
