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
    pub fn new(input: &str) -> Self {
        let mut i: Input = match serde_json::from_str(input) {
            Ok(e) => e,
            Err(e) => panic!("Error {:?}, unable to parse config file!", e),
        };
        if i.src_server.is_empty() {
            panic!("Error! source server not given!");
        }
        if i.dest_server.is_empty() {
            panic!("Error! destination server not given!");
        }
        if i.src_share.is_empty() {
            panic!("Error! source share not given!");
        }
        if i.dest_share.is_empty() {
            panic!("Error! destination share not given!");
        }
        //check if shares starts with '/', add if no
        if !i.src_share.starts_with('/') {
            debug!("Source share does not start with '/', adding '/'");
            i.src_share = format!("/{}", i.src_share);
        }
        if !i.dest_share.starts_with('/') {
            debug!("Destination share does not start with '/', adding '/'");
            i.dest_share = format!("/{}", i.dest_share);
        }
        match i.system {
            FileSystemType::Nfs => {
                // if the input is empty, default to '/'
                if i.src_path.to_string_lossy().is_empty() {
                    debug!("Empty source path, defaulting to /");
                    i.src_path = PathBuf::from("/")
                }
                if i.dest_path.to_string_lossy().is_empty() {
                    debug!("Empty destination path, defaulting to /");
                    i.dest_path = PathBuf::from("/")
                }
            }
            FileSystemType::Samba => {
                // if the input is not an smburl, default to 'smb://server/share'
                if !is_smb_path(&i.src_path) {
                    error!("Improperly formatted source path, defaulting to smb://server/share");
                    i.src_path = PathBuf::from(format!("smb://{}{}", i.src_server, i.src_share));
                }
                if !is_smb_path(&i.dest_path) {
                    error!(
                        "Improperly formatted destination path, defaulting to smb://server/share"
                    );
                    i.dest_path = PathBuf::from(format!("smb://{}{}", i.dest_server, i.dest_share));
                }
            }
        }
        i
    }
}
