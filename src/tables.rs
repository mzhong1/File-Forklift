use crate::error::*;
use crate::rsync::SyncStats;
use crate::socket_node::*;

use chrono::NaiveDateTime;
use lazy_static::*;
use postgres::*;
use postgres_derive::*;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Mutex;

lazy_static! {
    pub static ref CURRENT_SOCKET: Mutex<Vec<SocketNode>> = {
        Mutex::new(vec![SocketNode::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            8080,
        ))])
    };
}

#[derive(Debug, ToSql, FromSql, Clone, PartialEq)]
#[postgres(name = "ErrorType")]
pub enum ErrorType {
    #[postgres(name = "IoError")]
    IoError,
    #[postgres(name = "SystemTimeError")]
    SystemTimeError,
    #[postgres(name = "NanomsgError")]
    NanomsgError,
    #[postgres(name = "AddrParseError")]
    AddrParseError,
    #[postgres(name = "SmbcError")]
    SmbcError,
    #[postgres(name = "FromUtf16Error")]
    FromUtf16Error,
    #[postgres(name = "FromUtf8Error")]
    FromUtf8Error,
    #[postgres(name = "StringParseError")]
    StringParseError,
    #[postgres(name = "IpLocalError")]
    IpLocalError,
    #[postgres(name = "InvalidConfigError")]
    InvalidConfigError,
    #[postgres(name = "FSError")]
    FSError,
    #[postgres(name = "RecvError")]
    RecvError,
    #[postgres(name = "SerdeJsonError")]
    SerdeJsonError,
    #[postgres(name = "ChecksumError")]
    ChecksumError,
}
#[derive(Debug, Clone)]
pub struct ErrorLog {
    node_id: Nodes,
    failure_id: ErrorType,
    reason: String,
    timestamp: NaiveDateTime,
}

impl ErrorLog {
    pub fn new(
        node_id: Nodes,
        failure_id: ErrorType,
        reason: String,
        timestamp: NaiveDateTime,
    ) -> Self {
        ErrorLog {
            node_id,
            failure_id,
            reason,
            timestamp,
        }
    }
}
#[derive(Debug, Clone, ToSql, FromSql)]
pub struct Nodes {
    node_ip: String, //as inet?
    node_port: i32,  //since u16 is not available in postgres
    node_status: NodeStatus,
    last_updated: NaiveDateTime,
}

impl Nodes {
    pub fn new(
        node_ip: String,
        node_port: i32,
        node_status: NodeStatus,
        last_updated: NaiveDateTime,
    ) -> Self {
        Nodes {
            node_ip,
            node_port,
            node_status,
            last_updated,
        }
    }
}

#[derive(Debug, Clone, ToSql, FromSql)]
pub enum NodeStatus {
    NodeAdded,
    NodeDied,
    NodeFinished,
}

#[derive(Debug, Clone, ToSql, FromSql)]
pub struct TotalSync {
    //node_id: Nodes,
    total_files: i64,
    total_size: i64,
    num_synced: i64,
    up_to_date: i64,
    copied: i64,
    symlink_created: i64,
    symlink_updated: i64,
    symlink_skipped: i64,
    permissions_updated: i64,
    checksum_updated: i64,
    directory_created: i64,
    directory_updated: i64,
}

impl TotalSync {
    pub fn new(
        //node_id: Nodes,
        stats: &SyncStats
    ) -> Self {
        TotalSync {
            // node_id,
            total_files: stats.tot_files as i64,
            total_size: stats.tot_size as i64,
            num_synced: stats.num_synced as i64,
            up_to_date: stats.up_to_date as i64,
            copied: stats.copied as i64,
            symlink_created: stats.symlink_created as i64,
            symlink_updated: stats.symlink_updated as i64,
            symlink_skipped: stats.symlink_skipped as i64,
            permissions_updated: stats.permissions_update as i64,
            checksum_updated: stats.checksum_updated as i64,
            directory_created: stats.directory_created as i64,
            directory_updated: stats.directory_updated as i64,
        }
    }
}

#[derive(Debug, Clone, ToSql, FromSql)]
pub struct Files {
    path: String, //Share/Path
    // node_id: Nodes,
    src_checksum: Vec<u8>,
    dest_checksum: Vec<u8>,
    size: i64,
    last_modified_time: NaiveDateTime,
}

impl Files {
    pub fn new(
        path: String,
        src_checksum: Vec<u8>,
        dest_checksum: Vec<u8>,
        size: i64,
        last_modified_time: NaiveDateTime,
    ) -> Self {
        Files {
            path,
            src_checksum,
            dest_checksum,
            size,
            last_modified_time,
        }
    }
}

// create ErrorTypes Table
pub fn init_errortypes(conn: &Connection) {
    conn.execute(
        "DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'ErrorType') THEN
            CREATE TYPE \"ErrorType\" AS ENUM (
            'IoError',
            'SystemTimeError', 
            'NanomsgError', 
            'AddrParseError', 
            'SmbcError',
            'FromUtf16Error',
            'FromUtf8Error',
            'StringParseError',
            'IpLocalError',
            'InvalidConfigError',
            'FSError',
            'RecvError', 
            'SerdeJsonError',
            'ChecksumError');
            END IF;
        END
        $$",
        &[],
    )
    .unwrap();
}

pub fn init_nodetable(conn: &Connection) {
    //let updates = vec![UpdateType::NodeAdded, UpdateType::NodeDied];
    conn.execute(
        "DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'NodeStatus') THEN
            CREATE TYPE \"NodeStatus\" AS ENUM (
            'NodeAdded',
            'NodeDied',
            'NodeFinished');
            END IF;
        END
        $$",
        &[],
    )
    .unwrap();
    let state = "CREATE TABLE IF NOT EXISTS Nodes (
        node_id BIGSERIAL UNIQUE,
        ip TEXT,
        port INT,
        node_status \"NodeStatus\",
        last_updated TIMESTAMP,
        PRIMARY KEY (ip, port))";
    conn.execute(state, &[]).unwrap();
}

pub fn init_errorlog(conn: &Connection) {
    let state = "CREATE TABLE IF NOT EXISTS ErrorLog (
        entry_num BIGSERIAL UNIQUE PRIMARY KEY,
        node_id BIGINT REFERENCES Nodes(node_id),
        failure_id \"ErrorType\",
        reason text,
        timestamp TIMESTAMP)";
    conn.execute(state, &[]).unwrap();
}

pub fn init_files(conn: &Connection) {
    let state = "CREATE TABLE IF NOT EXISTS Files (
        path text UNIQUE PRIMARY KEY,
        node_id BIGINT REFERENCES Nodes(node_id),
        src_checksum BYTEA,
        dest_checksum BYTEA,
        size BIGINT,
        last_modified_time TIMESTAMP)";
    conn.execute(state, &[]).unwrap();
}

pub fn init_totalsync(conn: &Connection) {
    let state = "CREATE TABLE IF NOT EXISTS TotalSync(
        node_id BIGINT UNIQUE PRIMARY KEY REFERENCES Nodes(node_id),
        total_files BIGINT,
        total_size BIGINT,
        num_synced BIGINT,
        up_to_date BIGINT,
        copied BIGINT,
        symlink_created BIGINT,
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
    println!("ErrorTypes Created!");
    init_nodetable(&conn);
    println!("Nodes Created!");
    init_errorlog(&conn);
    println!("ErrorLog Created!");
    init_files(&conn);
    println!("Files Created!");
    init_totalsync(&conn);
    println!("TotalSync Created!");
    conn
}

pub fn set_current_node(node: &SocketNode) {
    let mut n = CURRENT_SOCKET.lock().unwrap();
    n.pop();
    n.push(node.clone());
}

pub fn get_current_node() -> ForkliftResult<SocketNode> {
    let n = CURRENT_SOCKET.lock().unwrap();
    match n.get(0) {
        Some(e) => Ok(e.clone()),
        None => Err(ForkliftError::FSError("Lazy_static is empty!".to_string())),
    }
}

/// update Nodelist
pub fn update_nodes(node: &Nodes, conn: &Connection) {
    conn.execute(
        "INSERT INTO Nodes(ip, port, node_status, last_updated) VALUES($1, $2, $3, $4)
        ON CONFLICT (ip, port) DO UPDATE SET node_status = $3, last_updated = $4 WHERE nodes.ip = $1 AND nodes.port = $2",
        &[
            &node.node_ip,
            &node.node_port,
            &node.node_status,
            &node.last_updated,
        ],
    )
    .unwrap();
}

pub fn get_node_id(node: &SocketNode, conn: &Connection) -> i64 {
    let mut val = -1;
    let ip = node.get_ip().to_string();
    let port = node.get_port() as i32;
    for row in &conn
        .query(
            "SELECT node_id FROM Nodes WHERE nodes.ip = $1 AND nodes.port = $2",
            &[&ip, &port],
        )
        .unwrap()
    {
        val = row.get(0);
    }
    val
}

/// log Error Node failures (log pretty much all of them)
pub fn log_errorlog(failure: &ErrorLog, conn: &Connection) -> ForkliftResult<()> {
    let socket = get_current_node()?;
    let node_id = get_node_id(&socket, conn);
    conn.execute(
        "INSERT INTO ErrorLog(node_id, failure_id, reason, timestamp) VALUES ($1, $2, $3, $4)",
        &[
            &node_id,
            &failure.failure_id,
            &failure.reason,
            &failure.timestamp,
        ],
    )?;
    Ok(())
}

/// update totalsync
pub fn update_totalsync(stat: &TotalSync, conn: &Connection) -> ForkliftResult<()> {
    let socket = get_current_node()?;
    let node_id = get_node_id(&socket, conn);
    conn.execute(
        "INSERT INTO TotalSync(node_id, total_files, total_size, num_synced, up_to_date, copied, symlink_created, symlink_updated, symlink_skipped, permissions_updated, checksum_updated, directory_created, directory_updated) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        ON CONFLICT (node_id) DO UPDATE SET total_files = $2, total_size = $3, num_synced = $4, up_to_date = $5, copied = $6, symlink_created = $7, symlink_updated = $8, symlink_skipped = $9, permissions_updated = $10, checksum_updated = $11, directory_created = $12, directory_updated = $13 WHERE totalsync.node_id = $1",
        &[
            &node_id,
            &stat.total_files,
            &stat.total_size,
            &stat.num_synced,
            &stat.up_to_date,
            &stat.copied,
            &stat.symlink_created,
            &stat.symlink_updated,
            &stat.symlink_skipped,
            &stat.permissions_updated,
            &stat.checksum_updated,
            &stat.directory_created,
            &stat.directory_updated,
        ],
    )?;
    Ok(())
}

/// update Files table
pub fn update_files(file: &Files, conn: &Connection) -> ForkliftResult<()> {
    let socket = get_current_node()?;
    let node_id = get_node_id(&socket, conn);
    conn.execute("INSERT INTO Files(path, node_id, src_checksum, dest_checksum, size, last_modified_time) VALUES($1, $2, $3, $4, $5, $6)
        ON CONFLICT (path) DO UPDATE SET node_id = $2, src_checksum = $3, dest_checksum = $4, size = $5, last_modified_time = $6 WHERE files.path = $1",
         &[&file.path,
         &node_id,
         &file.src_checksum,
         &file.dest_checksum,
         &file.size,
         &file.last_modified_time])?;
    Ok(())
}
