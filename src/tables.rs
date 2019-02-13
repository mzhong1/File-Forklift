use std::net::SocketAddr;
use chrono::{DateTime, Utc};
use error::ForkliftError;
use std::path::PathBuf
use postgres::Connection;
use postgres_derive::*;

#[derive(Debug, ToSql, FromSql)]
struct TransferProgress{
    entry_id: u64,
    source_node_id: SourceNodes,
    timestamp: DateTime<Utc>,
    file_path: PathBuf,
    checksum: Vec<u8>,
    size: u64,
    size_migrated: u64,
}

#[derive(Debug, ToSql, FromSql)]
enum FailureID{
    IoError = 0,
    SystemTimeError,
    NanomsgError,
    AddrParseError,
    SmbcError,
    FromUtf16Error,
    FromUtf8Error,
    StringParseError
    IpLocalError,
    InvalidConfigError,
    FSError,
    RecvError,
    SerdeJsonError,
}
#[derive(Debug, ToSql, FromSql)]
struct NodeFailure{
    failure_id: FailureID,
    reason: String,
    source_node_id: SourceNodes,
    timestamp: DateTime<Utc>,
}
#[derive(Debug, ToSql, FromSql)]
struct SourceNodes{
    souce_node_id: u64,
    node_ip: SocketAddr,
}


pub fn sendError(err: ForkliftError, reason: String, conn: Connection){

}