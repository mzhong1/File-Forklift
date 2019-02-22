use crate::error::{ForkliftError, ForkliftResult};
use crate::filesystem::FileSystemType;

use serde_derive::*;
use std::net::SocketAddr;
use std::path::PathBuf;

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
    pub debug_level: u32,
    /// The number of threads used in the processing
    pub num_threads: u32,
    /// The workgroup of the user (Please default to WORKGROUP if not using Samba)
    #[serde(default = "default_workgroup")]
    pub workgroup: String,
    /// NFS is always "/" (unless using subdirectory),
    /// Samba smb url to root or subdirectory (if Glusterfs, MUST be subdirectory)
    pub src_path: PathBuf,
    /// NFS is always "/" (unless using subdirectory),
    /// Samba smb url to root or subdirectory (if Glusterfs, MUST be subdirectory)
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
/// Check if given path is an smburl
/// NOTE: the smburl format is smb://server/share.  Other
/// smburl syntax, such as smb://username:password@server/share will not
/// work, though they will pass the is_smb_path test, as whether or not
/// the url will work depends entirely on the smbclient you are using.
/// In general, refrain from using additional parts and input them in
/// the command line as opposed to in the smburl for security at the very least.  
pub fn is_smb_path(path: &PathBuf) -> bool {
    let p = path.to_string_lossy().into_owned();
    // Why 8?  6 for smb://, + 1 for minimum server name + 1 for /, + 1 for minimum share name
    if p.len() < 9 {
        return false;
    }
    let (prefix, suffix) = p.split_at(6);
    prefix == "smb://"
        && match suffix.find('/') {
            Some(0) => false,
            Some(_) => true,
            None => false,
        }
}

impl Input {
    //NOTE, send invalid config error when panicking
    /// create new Input object from config file
    pub fn new_input(config: &str) -> ForkliftResult<Self> {
        let input: Input = match serde_json::from_str(config) {
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
                // if the input is not an smburl, 'smb://server/share', exit
                if !is_smb_path(&input.src_path) {
                    return Err(ForkliftError::InvalidConfigError(
                        "Improperly formatted source path, should be smb://server/share"
                            .to_string(),
                    ));
                }
                if !is_smb_path(&input.dest_path) {
                    return Err(ForkliftError::InvalidConfigError(
                        "Improperly formatted destination path, should beto smb://server/share"
                            .to_string(),
                    ));
                }
            }
        }
        Ok(input)
    }
}
