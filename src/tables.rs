use std::net::SocketAddr;
use chrono::{DateTime, Utc};
use error::ForkliftError;
use std::path::PathBuf

struct TransferProgress{
    entry_id: u64,
    source_node_id: SourceNodes,
    timestamp: DateTime<Utc>,
    file_path: PathBuf,
    checksum: 
}


struct NodeFailure{
    failure_id: u64,
    source_node_id: SourceNodes,
    reason: ForkliftError,

}

struct SourceNodes{
    souce_node_id: u64,
    node_ip: SocketAddr,
}