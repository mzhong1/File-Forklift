//! The errors

use crossbeam::channel::RecvError;
use derive_error as de;
use nng::Error as NanomsgError;
use postgres::Error as PostgresError;
use protobuf::ProtobufError;
use r2d2::Error as R2D2Error;
use serde_json::Error as SerdeJsonError;
use rust_smb::SmbcError;
use std::io::Error as IoError;
use std::net::AddrParseError;
use std::string::{FromUtf16Error, FromUtf8Error, ParseError as StringParseError};
use std::time::SystemTimeError;

/// custom Result type for Filesystem Forklift
pub type ForkliftResult<T> = Result<T, ForkliftError>;

#[derive(Debug, de::Error)]
pub enum ConvertStringError {
    FromUtf16Error(FromUtf16Error),
    FromUtf8Error(FromUtf8Error),
    StringParseError(StringParseError),
}

#[derive(Debug, de::Error)]
/// custom error types for Filesystem Forklift
pub enum ForkliftError {
    /// Failed to parse Socket Address
    AddrParseError(AddrParseError),
    #[error(msg_embedded, non_std, no_from)]
    CLIError(String),
    #[error(msg_embedded, non_std, no_from)]
    ChecksumError(String),
    /// Failed to convert String to/from bytes
    ConvertStringError(ConvertStringError),
    #[error(msg_embedded, non_std, no_from)]
    CrossbeamChannelError(String),
    #[error(msg_embedded, non_std, no_from)]
    FSError(String),
    #[error(msg_embedded, non_std, no_from)]
    HeartbeatError(String),
    #[error(msg_embedded, non_std, no_from)]
    InvalidConfigError(String),
    IoError(IoError),
    #[error(msg_embedded, non_std, no_from)]
    IpLocalError(String),
    /// Nanomsg Error
    NanomsgError(NanomsgError),
    /// Postgres Error
    PostgresError(PostgresError),
    /// Protobuf Error
    ProtobufError(ProtobufError),
    /// Crossbeam receiver error
    RecvError(RecvError),
    /// Config Parsing Error
    SerdeJsonError(SerdeJsonError),
    /// Samba Error
    SmbcError(SmbcError),
    /// System Time Error
    SystemTimeError(SystemTimeError),
    #[error(msg_embedded, non_std, no_from)]
    TimeoutError(String),
    /// R2D2 error
    R2D2Error(R2D2Error),
}

impl From<FromUtf16Error> for ForkliftError {
    fn from(err: FromUtf16Error) -> ForkliftError {
        ForkliftError::ConvertStringError(ConvertStringError::FromUtf16Error(err))
    }
}

impl From<FromUtf8Error> for ForkliftError {
    fn from(err: FromUtf8Error) -> ForkliftError {
        ForkliftError::ConvertStringError(ConvertStringError::FromUtf8Error(err))
    }
}

impl From<StringParseError> for ForkliftError {
    fn from(err: StringParseError) -> ForkliftError {
        ForkliftError::ConvertStringError(ConvertStringError::StringParseError(err))
    }
}
