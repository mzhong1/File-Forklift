use crate::error::*;
use crate::rsync::SyncStats;
use crate::socket_node::*;

use chrono::{NaiveDateTime, Utc};
use lazy_static::*;
use log::*;
use postgres::*;
use postgres_derive::*;
use r2d2::{Pool, PooledConnection};
use r2d2_postgres::{PostgresConnectionManager, TlsMode};

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Mutex;
use std::time::Duration;

lazy_static! {
    /// hold the current machine's Socket Address
    pub static ref CURRENT_SOCKET: Mutex<Vec<SocketNode>> = {
        Mutex::new(vec![SocketNode::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            8080,
        ))])
    };
}

#[derive(Debug, ToSql, FromSql, Copy, Clone, PartialEq)]
#[postgres(name = "ErrorType")]
/// Usable ErrorTypes logged in Postgres
pub enum ErrorType {
    AddrParseError,
    ChecksumError,
    CrossbeamChannelError,
    FromUtf16Error,
    FromUtf8Error,
    FSError,
    HeartbeatError,
    InvalidConfigError,
    IoError,
    IpLocalError,
    NanomsgError,
    PoisonedMutexError,
    PostgresError,
    ProtobufError,
    RecvError,
    SerdeJsonError,
    SmbcError,
    StringParseError,
    SystemTimeError,
    TimeoutError,
}
#[derive(Debug, Clone)]
/// an ErrorLog table entry
pub struct ErrorLog {
    failure_id: ErrorType,
    reason: String,
    timestamp: NaiveDateTime,
}

impl ErrorLog {
    /// create a new ErrorLog
    pub fn new(failure_id: ErrorType, reason: &str, timestamp: NaiveDateTime) -> Self {
        ErrorLog { failure_id, reason: reason.to_string(), timestamp }
    }
    /// create a new ErrorLog from a ForkliftError
    pub fn from_err(err: &ForkliftError, timestamp: NaiveDateTime) -> Self {
        let failure_id = match err {
            ForkliftError::AddrParseError(_) => ErrorType::AddrParseError,
            ForkliftError::ChecksumError(_) => ErrorType::ChecksumError,
            ForkliftError::ConvertStringError(ConvertStringError::FromUtf16Error(_)) => {
                ErrorType::FromUtf16Error
            }
            ForkliftError::ConvertStringError(ConvertStringError::FromUtf8Error(_)) => {
                ErrorType::FromUtf8Error
            }
            ForkliftError::ConvertStringError(ConvertStringError::StringParseError(_)) => {
                ErrorType::StringParseError
            }
            ForkliftError::FSError(_) => ErrorType::FSError,
            ForkliftError::InvalidConfigError(_) => ErrorType::InvalidConfigError,
            ForkliftError::IoError(_) => ErrorType::IoError,
            ForkliftError::IpLocalError(_) => ErrorType::IpLocalError,
            ForkliftError::NanomsgError(_) => ErrorType::NanomsgError,
            ForkliftError::PostgresError(_) => ErrorType::PostgresError,
            ForkliftError::RecvError(_) => ErrorType::RecvError,
            ForkliftError::SerdeJsonError(_) => ErrorType::SerdeJsonError,
            ForkliftError::SmbcError(_) => ErrorType::SmbcError,
            ForkliftError::SystemTimeError(_) => ErrorType::SystemTimeError,
            ForkliftError::CrossbeamChannelError(_) => ErrorType::CrossbeamChannelError,
            ForkliftError::TimeoutError(_) => ErrorType::TimeoutError,
            ForkliftError::HeartbeatError(_) => ErrorType::HeartbeatError,
            ForkliftError::CLIError(_) => ErrorType::InvalidConfigError,
            ForkliftError::ProtobufError(_) => ErrorType::ProtobufError,
            ForkliftError::R2D2Error(_) => ErrorType::PostgresError,
        };
        let reason = format!("{:?}", err);
        ErrorLog { failure_id, reason, timestamp }
    }
}
#[derive(Debug, Clone, ToSql, FromSql)]
/// Node table entry
pub struct Nodes {
    node_ip: String,
    node_port: i32,
    node_status: NodeStatus,
    last_updated: NaiveDateTime,
}

impl Nodes {
    /// create a new Nodes
    pub fn new_all(
        node_ip: &str,
        node_port: i32,
        node_status: NodeStatus,
        last_updated: NaiveDateTime,
    ) -> Self {
        Nodes { node_ip: node_ip.to_string(), node_port, node_status, last_updated }
    }
    /// create a new Nodes from a NodeStatus
    pub fn new(node_status: NodeStatus) -> ForkliftResult<Self> {
        let socket = get_current_node()?;
        let last_updated = current_time();
        Ok(Nodes::new_all(
            &socket.get_ip().to_string(),
            i32::from(socket.get_port()),
            node_status,
            last_updated,
        ))
    }
}

#[derive(Debug, Clone, ToSql, FromSql)]
/// The current state of a node
pub enum NodeStatus {
    NodeAdded,
    NodeDied,
    NodeFinished,
}

#[derive(Debug, Clone, Copy, ToSql, FromSql)]
/// entry for TotalSync table
pub struct TotalSync {
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
    /// create a new TotalSync from SyncStats
    pub fn new(stats: &SyncStats) -> Self {
        TotalSync {
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
/// entry to Files table
pub struct Files {
    /// path of File in the format src_share/path
    path: String, //Share/Path
    src_checksum: Vec<u8>,
    dest_checksum: Vec<u8>,
    size: i64,
    last_modified_time: NaiveDateTime,
}

impl Files {
    /// create a new Files
    pub fn new(
        path: &str,
        src_checksum: Vec<u8>,
        dest_checksum: Vec<u8>,
        size: i64,
        last_modified_time: NaiveDateTime,
    ) -> Self {
        Files { path: path.to_string(), src_checksum, dest_checksum, size, last_modified_time }
    }
}

// create ErrorTypes Enum
pub fn init_errortypes(conn: &Connection) -> ForkliftResult<()> {
    conn.execute(
        "DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'ErrorType') THEN
            CREATE TYPE \"ErrorType\" AS ENUM (
            'AddrParseError',
            'ChecksumError',
            'CrossbeamChannelError',
            'FromUtf16Error',
            'FromUtf8Error',
            'FSError',
            'HeartbeatError',
            'InvalidConfigError',
            'IoError',
            'IpLocalError',
            'NanomsgError',
            'PoisonedMutexError',
            'PostgresError',
            'ProtobufError',
            'RecvError',
            'SerdeJsonError',
            'SmbcError',
            'StringParseError',
            'SystemTimeError',
            'TimeoutError');
            END IF;
        END
        $$",
        &[],
    )?;
    Ok(())
}

/// create Nodes table
pub fn init_nodetable(conn: &Connection) -> ForkliftResult<()> {
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
    )?;
    let state = "CREATE TABLE IF NOT EXISTS Nodes (
        node_id BIGSERIAL UNIQUE,
        ip TEXT,
        port INT,
        node_status \"NodeStatus\",
        last_updated TIMESTAMP,
        PRIMARY KEY (ip, port))";
    conn.execute(state, &[])?;
    Ok(())
}

/// create ErrorLog table
pub fn init_errorlog(conn: &Connection) -> ForkliftResult<()> {
    let state = "CREATE TABLE IF NOT EXISTS ErrorLog (
        entry_num BIGSERIAL UNIQUE PRIMARY KEY,
        node_id BIGINT REFERENCES Nodes(node_id),
        failure_id \"ErrorType\",
        reason text,
        timestamp TIMESTAMP)";
    conn.execute(state, &[])?;
    Ok(())
}

/// create Files table
pub fn init_files(conn: &Connection) -> ForkliftResult<()> {
    let state = "CREATE TABLE IF NOT EXISTS Files (
        path text UNIQUE PRIMARY KEY,
        node_id BIGINT REFERENCES Nodes(node_id),
        src_checksum BYTEA,
        dest_checksum BYTEA,
        size BIGINT,
        last_modified_time TIMESTAMP)";
    conn.execute(state, &[])?;
    Ok(())
}

/// create TotalSync table
pub fn init_totalsync(conn: &Connection) -> ForkliftResult<()> {
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
    conn.execute(state, &[])?;
    Ok(())
}

/// initialize connection to postgres database and initialize all tables
pub fn init_connection(path: &str) -> ForkliftResult<Pool<PostgresConnectionManager>> {
    let manager = PostgresConnectionManager::new(path, TlsMode::None)?;
    let pool = r2d2::Pool::builder()
        .max_size(10)
        .connection_timeout(Duration::from_secs(300))
        .build(manager)?;
    let conn = pool.get()?;
    init_errortypes(&conn)?;
    debug!("ErrorTypes Created!");
    init_nodetable(&conn)?;
    debug!("Nodes Created!");
    init_errorlog(&conn)?;
    debug!("ErrorLog Created!");
    init_files(&conn)?;
    debug!("Files Created!");
    init_totalsync(&conn)?;
    debug!("TotalSync Created!");
    Ok(pool)
}

/// set the current node to this machine's socket address
pub fn set_current_node(node: &SocketNode) -> ForkliftResult<()> {
    let mut n = match CURRENT_SOCKET.lock() {
        Ok(list) => list,
        Err(e) => {
            error!("Error {:?}", e);
            return Err(ForkliftError::FSError(
                "Poison Error! unable to set current node!".to_string(),
            ));
        }
    };
    n.pop();
    n.push(*node);
    Ok(())
}

/// get the current node's socket address
pub fn get_current_node() -> ForkliftResult<SocketNode> {
    let n = match CURRENT_SOCKET.lock() {
        Ok(list) => list,
        Err(e) => {
            error!("Error: {:?}", e);
            return Err(ForkliftError::FSError(
                "Poison Error! Unable to get current node".to_string(),
            ));
        }
    };
    match n.get(0) {
        Some(e) => Ok(*e),
        None => Err(ForkliftError::FSError("Lazy_static is empty!".to_string())),
    }
}

/// update Nodes Table
/// If current node is Finished, then can only change if node becomes Active
/// otherwise, store the most recent change message
pub fn update_nodes(node: &Nodes, conn: &Connection) -> ForkliftResult<()> {
    if let NodeStatus::NodeDied = node.node_status {
        let mut status: NodeStatus = NodeStatus::NodeAdded;
        for row in &conn.query(
            "SELECT node_status FROM Nodes WHERE ip = $1 AND port = $2",
            &[&node.node_ip, &node.node_port],
        )? {
            status = row.get(0);
        }
        if let NodeStatus::NodeFinished = status {
            return Ok(());
        }
    }
    conn.execute(
        "INSERT INTO Nodes(ip, port, node_status, last_updated) VALUES($1, $2, $3, $4)
        ON CONFLICT (ip, port) DO UPDATE SET node_status = $3, last_updated = $4 WHERE nodes.ip = $1 AND nodes.port = $2",
        &[
            &node.node_ip,
            &node.node_port,
            &node.node_status,
            &node.last_updated,
        ],
    )?;
    Ok(())
}

/// given a socketNode, get the matching node_id from the Nodes table
pub fn get_node_id(node: &SocketNode, conn: &Connection) -> ForkliftResult<i64> {
    let mut val = -1;
    let ip = node.get_ip().to_string();
    let port = i32::from(node.get_port());
    for row in &conn
        .query("SELECT node_id FROM Nodes WHERE nodes.ip = $1 AND nodes.port = $2", &[&ip, &port])?
    {
        val = row.get(0);
    }
    Ok(val)
}

/// log Error Node failures (log pretty much all of them)
pub fn log_errorlog(failure: &ErrorLog, conn: &Connection) -> ForkliftResult<()> {
    let socket = get_current_node()?;
    let node_id = get_node_id(&socket, conn)?;
    conn.execute(
        "INSERT INTO ErrorLog(node_id, failure_id, reason, timestamp) VALUES ($1, $2, $3, $4)",
        &[&node_id, &failure.failure_id, &failure.reason, &failure.timestamp],
    )?;
    Ok(())
}

/// update totalsync
pub fn update_totalsync(stat: &TotalSync, conn: &Connection) -> ForkliftResult<()> {
    let socket = get_current_node()?;
    let node_id = get_node_id(&socket, conn)?;
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
    let node_id = get_node_id(&socket, conn)?;
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

/// wrapper for update_files
pub fn post_update_files(
    file: &Files,
    conn: &Option<PooledConnection<PostgresConnectionManager>>,
) -> ForkliftResult<()> {
    if let Some(e) = conn {
        update_files(&file, &e)?
    }
    Ok(())
}

/// wrapper for update_totalsync
pub fn post_update_totalsync(
    stat: &SyncStats,
    conn: &Option<PooledConnection<PostgresConnectionManager>>,
) -> ForkliftResult<()> {
    if let Some(e) = conn {
        let tot_stat = TotalSync::new(&stat);
        update_totalsync(&tot_stat, e)?;
    }
    Ok(())
}

/// wrapper for update_nodes
pub fn post_update_nodes(
    status: &Nodes,
    conn: &Option<PooledConnection<PostgresConnectionManager>>,
) -> ForkliftResult<()> {
    if let Some(e) = conn {
        update_nodes(&status, &e)?;
    }
    Ok(())
}

/// post an ErrorType error
pub fn post_err(
    err_type: ErrorType,
    reason: &str,
    conn: &Option<PooledConnection<PostgresConnectionManager>>,
) -> ForkliftResult<()> {
    error!("{}", reason);
    if let Some(e) = &conn {
        let fail = ErrorLog::new(err_type, reason, current_time());
        log_errorlog(&fail, &e)?;
    }
    Ok(())
}

/// post a ForkliftError
pub fn post_forklift_err(
    err: &ForkliftError,
    conn: &Option<PooledConnection<PostgresConnectionManager>>,
) -> ForkliftResult<()> {
    error!("{:?}", err);
    if let Some(c) = &conn {
        let fail = ErrorLog::from_err(err, current_time());
        log_errorlog(&fail, &c)?;
    }
    Ok(())
}

/// get the current time
pub fn current_time() -> NaiveDateTime {
    let now = Utc::now();
    NaiveDateTime::from_timestamp(now.timestamp(), now.timestamp_subsec_nanos())
}
