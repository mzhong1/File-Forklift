use crate::error::ForkliftError;
use chrono::NaiveDateTime;
use postgres::*;
use postgres_derive::*;

#[derive(Debug, Clone, ToSql, FromSql)]
struct TransferProgress {
    entry_id: i64,
    source_node_id: SourceNodes,
    timestamp: NaiveDateTime,
    file_path: String,
    checksum: Vec<u8>,
    size: i64,
    size_migrated: i64,
}

#[derive(Debug, ToSql, FromSql, Clone)]
enum FailureID {
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
}
#[derive(Debug, Clone)]
struct NodeFailure {
    failure_id: FailureID,
    reason: String,
    source_node_id: SourceNodes,
    timestamp: NaiveDateTime,
}
#[derive(Debug, Clone, ToSql, FromSql)]
struct SourceNodes {
    souce_node_id: i64,
    node_address: String,
}

pub fn init_connection(path: String) -> Connection {
    let conn = Connection::connect(path, TlsMode::None).unwrap();
    let state = "CREATE TABLE IF NOT EXISTS ErrorTypes (
        failure_id FailureID UNIQUE PRIMARY KEY )";
    conn.execute(state, &[]).unwrap();
    conn.execute(
        "INSERT INTO ErrorTypes (failure_id) VALUES ($1)",
        &[&FailureID::IoError],
    );
    //let state = "INSERT INTO NodeFailures (failure_id) VALUES ($1)", FailureID::IoError;
    conn
}

pub fn sendError(err: ForkliftError, reason: String, conn: Connection) {}
