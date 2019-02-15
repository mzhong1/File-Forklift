use crate::rsync::SyncStats;

use chrono::NaiveDateTime;
use postgres::*;
use postgres_derive::*;

#[derive(Debug, Clone, ToSql, FromSql)]
pub struct TransferProgress {
    node_id: Nodes,
    file_path: String,
    checksum: Vec<u8>,
    size: i64,
    size_migrated: i64,
    timestamp: NaiveDateTime,
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
#[derive(Debug, Clone, ToSql, FromSql)]
pub struct Nodes {
    node_ip: String, //as inet?
    node_port: i32,  //since u16 is not available in postgres
    node_status: NodeStatus,
    update_time: NaiveDateTime,
}

#[derive(Debug, Clone, ToSql, FromSql)]
pub enum NodeStatus {
    NodeAdded,
    NodeDied,
    NodeFinished,
}

#[derive(Debug, Clone, ToSql, FromSql)]
pub struct TotalSync {
    node_id: i64, //Nodes
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

#[derive(Debug, Clone, ToSql, FromSql)]
pub struct Files {
    path: String,
    node_id: Nodes,
    src_checksum: Vec<u8>,
    dest_checksum: Vec<u8>,
    size: i64,
    last_modified_time: NaiveDateTime,
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
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'UpdateType') THEN
            CREATE TYPE \"UpdateType\" AS ENUM (
            'NodeAdded',
            'NodeDied');
            END IF;
        END
        $$",
        &[],
    )
    .unwrap();
    let state = "CREATE TABLE IF NOT EXISTS Nodes (
        node_id BIGSERIAL UNIQUE PRIMARY KEY,
        ip INET,
        port INT,
        node_status \"UpdateType\",
        last_updated TIMESTAMP)";
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

pub fn init_transferprogress(conn: &Connection) {
    let state = "CREATE TABLE IF NOT EXISTS TransferProgress (
        node_id BIGINT UNIQUE PRIMARY KEY REFERENCES Nodes(node_id),
        path text,
        checksum BYTEA,
        size BIGINT,
        size_migrated BIGINT,
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
    println!("ErrorTypes Created!");
    init_nodetable(&conn);
    println!("Nodes Created!");
    init_errorlog(&conn);
    println!("ErrorLog Created!");
    init_files(&conn);
    println!("Files Created!");
    //init_transferprogress(&conn);
    //println!("TransferProgress Created!");
    init_totalsync(&conn);
    println!("TotalSync Created!");
    conn
}

/// update Nodelist
pub fn update_nodes(node: Nodes, conn: &Connection) {
    let n = format!("{:?}", node);
    conn.execute("INSERT INTO Nodes (node_id) VALUES ($1)", &[&n])
        .unwrap();
}

/// log Error Node failures (log pretty much all of them)
pub fn log_errorlog(failure: ErrorLog, conn: &Connection) {}

/// update totalsync
pub fn update_totalsync(stat: SyncStats, conn: &Connection) {}

/// update Files table
pub fn update_files(file: Files, conn: &Connection) {}

/// update transfer progress
pub fn update_transferprogress(progress: TransferProgress, conn: &Connection) {}
