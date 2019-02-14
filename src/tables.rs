use crate::error::ForkliftError;
use crate::rsync::SyncStats;
use crate::socket_node::*;

use chrono::NaiveDateTime;
use postgres::*;
use postgres_derive::*;

#[derive(Debug, Clone, ToSql, FromSql)]
pub struct TransferProgress {
    entry_id: i64,
    source_node_id: String, //Nodes
    timestamp: NaiveDateTime,
    file_path: String,
    checksum: Vec<u8>,
    size: i64,
    size_migrated: i64,
}

#[derive(Debug, ToSql, FromSql, Clone)]
pub enum FailureID {
    IoError,
    SystemTimeError,
    NanomsgError,
    AddrParseError,
    SmbcError,
    FromUtf16Error,
    FromUtf8Error,
    StringParseError,
    IpLocalError,
    InvalidConfigError,
    FSError,
    RecvError,
    SerdeJsonError,
    ChecksumError,
}
#[derive(Debug, Clone)]
pub struct NodeFailure {
    failure_id: FailureID,
    reason: String,
    source_node_id: String, //Nodes
    timestamp: NaiveDateTime,
}
#[derive(Debug, Clone, ToSql, FromSql)]
struct Nodes {
    node_address: String, //as inet?
    node_port: i32,       //since u16 is not available in postgres
}

#[derive(Debug, Clone, ToSql, FromSql)]
enum UpdateType {
    NodeAdded,
    NodeDied,
}

#[derive(Debug, Clone, ToSql, FromSql)]
pub struct NodeStatus {
    //entry_num BIGSERIAL
    node_id: String, //Nodes
    update_type: UpdateType,
    update_time: NaiveDateTime,
}

// create ErrorTypes Table
pub fn init_errortypes(conn: &Connection) {
    let failures = vec![
        FailureID::AddrParseError,
        FailureID::FSError,
        FailureID::FromUtf16Error,
        FailureID::FromUtf8Error,
        FailureID::InvalidConfigError,
        FailureID::IoError,
        FailureID::IpLocalError,
        FailureID::NanomsgError,
        FailureID::RecvError,
        FailureID::SerdeJsonError,
        FailureID::SmbcError,
        FailureID::StringParseError,
        FailureID::SystemTimeError,
        FailureID::ChecksumError,
    ];
    let state = "CREATE TABLE IF NOT EXISTS ErrorTypes (
        failure_id FailureID UNIQUE PRIMARY KEY )";
    conn.execute(state, &[]).unwrap();
    let stmt = match conn.prepare("INSERT INTO ErrorTypes (failure_id) VALUES ($1)") {
        Ok(e) => e,
        Err(e) => {
            println!("Error! {:?}", e);
            return;
        }
    };
    for f_id in failures {
        match stmt.execute(&[&f_id]) {
            Ok(_) => (),
            Err(e) => println!("Error! {:?}", e),
        }
    }
}

pub fn init_nodetable(conn: &Connection) {
    let state = "CREATE TABLE IF NOT EXISTS Nodes (
        node_id text UNIQUE PRIMARY KEY )";
    conn.execute(state, &[]).unwrap();
}
pub fn init_nodestatus(conn: &Connection) {
    let state = "CREATE TABLE IF NOT EXISTS Nodes (
        entry_num BIGSERIAL UNIQUE PRIMARY KEY,
        node_id text REFERENCES Nodes(node_id),
        update_type UpdateType,
        timestamp Timestamp)";
    conn.execute(state, &[]).unwrap();
}

pub fn init_nodefailures(conn: &Connection) {
    let state = "CREATE TABLE IF NOT EXISTS NodeFailures (
        entry_num BIGSERIAL UNIQUE PRIMARY KEY,
        node_id text REFERENCES Nodes(node_id),
        failure_id FailureId REFERENCES ErrorTypes(failure_id),
        reason text,
        timestamp Timestamp)";
    conn.execute(state, &[]).unwrap();
}

pub fn init_transferProgress(conn: &Connection) {
    let state = "CREATE TABLE IF NOT EXISTS TransferProgress (
        entry_num BIGSERIAL UNIQUE PRIMARY KEY,
        node_id text REFERENCES Nodes(node_id),
        path text,
        checksum BYTEA,
        size BIGINT,
        size_migrated BIGINT,
        timestamp Timestamp)";
    conn.execute(state, &[]).unwrap();
}

pub fn init_totalsync(conn: &Connection) {
    let state = "CREATE TABLE IF NOT EXISTS TransferProgress (
        node_id text UNIQUE PRIMARY KEY REFERENCES Nodes(node_id),
        total_files BIGINT,
        total_size BIGINT,
        num_synced BIGINT,
        up_to_date BIGINT,
        copied BIGINT,
        symlink_create BIGINT,
        symlink_updated BIGINT,
        symlink_skipped BIGINT,
        permissions_updated BIGINT,
        checksum_updated BIGINT,
        directory_created BIGINT,
        directory_updated BIGINT,
        timestamp Timestamp)";
    conn.execute(state, &[]).unwrap();
}

pub fn init_connection(path: String) -> Connection {
    let conn = Connection::connect(path, TlsMode::None).expect("Cannot connect to database");
    init_errortypes(&conn);
    init_nodetable(&conn);
    init_nodestatus(&conn);
    init_nodefailures(&conn);
    init_transferProgress(&conn);
    init_totalsync(&conn);
    //let state = "INSERT INTO NodeFailures (failure_id) VALUES ($1)", FailureID::IoError;
    conn
}

pub fn send_error(err: ForkliftError, reason: String, conn: &Connection) {}

/// Add node to nodeList
pub fn add_node(node: SocketNode, conn: &Connection) {
    let n = format!("{:?}", node);
    conn.execute("INSERT INTO Nodes (node_id) VALUES ($1)", &[&n])
        .unwrap();
}

/// Log cluster status updates
pub fn log_nodestatus(status: NodeStatus, conn: &Connection) {}

/// log Error Node failures
pub fn log_nodefailure(failure: NodeFailure, conn: &Connection) {}

/// update
pub fn update_totalsync(stat: SyncStats, conn: &Connection) {}
