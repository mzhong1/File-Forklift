use crossbeam::channel::RecvError;
use nanomsg::Error as NanomsgError;
use postgres::Error as PostgresError;
use serde_json::Error as SerdeJsonError;
use smbc::Error as SmbcError;
use std::error::Error as err;
use std::fmt;
use std::io::Error as IoError;
use std::net::AddrParseError;
use std::string::{FromUtf16Error, FromUtf8Error, ParseError as StringParseError};
use std::time::SystemTimeError;

pub type ForkliftResult<T> = Result<T, ForkliftError>;

#[derive(Debug)]
pub enum ConvertStringError {
    FromUtf16Error(FromUtf16Error),
    FromUtf8Error(FromUtf8Error),
    StringParseError(StringParseError),
}

#[derive(Debug)]
pub enum ForkliftError {
    IoError(IoError),
    SystemTimeError(SystemTimeError),
    NanomsgError(NanomsgError),
    AddrParseError(AddrParseError),
    SmbcError(SmbcError),
    ConvertStringError(ConvertStringError),
    IpLocalError(String),
    InvalidConfigError(String),
    FSError(String),
    RecvError(RecvError),
    SerdeJsonError(SerdeJsonError),
    ChecksumError(String),
    PostgresError(PostgresError),
    CrossbeamChannelError(String),
    TimeoutError(String),
}

impl fmt::Display for ForkliftError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ForkliftError::InvalidConfigError(_) => f.write_str("InvalidConfigError"),
            ForkliftError::IpLocalError(_) => f.write_str("IpLocalError"),
            ForkliftError::FSError(_) => f.write_str("FileSystemError"),
            ForkliftError::ChecksumError(_) => f.write_str("ChecksumError"),
            ForkliftError::CrossbeamChannelError(_) => f.write_str("CrossbeamChannelError"),
            ForkliftError::TimeoutError(_) => f.write_str("TimeoutError"),
            _ => f.write_str(self.description()),
        }
    }
}

impl err for ForkliftError {
    fn description(&self) -> &str {
        match *self {
            ForkliftError::IoError(ref e) => e.description(),
            ForkliftError::SystemTimeError(ref e) => e.description(),
            ForkliftError::NanomsgError(ref e) => e.description(),
            ForkliftError::AddrParseError(ref e) => e.description(),
            ForkliftError::SmbcError(ref e) => e.description(),
            ForkliftError::ConvertStringError(ref c) => match c {
                ConvertStringError::FromUtf16Error(ref e) => e.description(),
                ConvertStringError::FromUtf8Error(ref e) => e.description(),
                ConvertStringError::StringParseError(ref e) => e.description(),
            },
            ForkliftError::IpLocalError(ref d) => &d,
            ForkliftError::InvalidConfigError(ref d) => &d,
            ForkliftError::FSError(ref d) => &d,
            ForkliftError::RecvError(ref e) => e.description(),
            ForkliftError::SerdeJsonError(ref e) => e.description(),
            ForkliftError::ChecksumError(ref d) => &d,
            ForkliftError::PostgresError(ref e) => e.description(),
            ForkliftError::CrossbeamChannelError(ref d) => &d,
            ForkliftError::TimeoutError(ref d) => &d,
        }
    }

    fn cause(&self) -> Option<&err> {
        match *self {
            ForkliftError::IoError(ref e) => e.cause(),
            ForkliftError::SystemTimeError(ref e) => e.cause(),
            ForkliftError::NanomsgError(ref e) => e.cause(),
            ForkliftError::AddrParseError(ref e) => e.cause(),
            ForkliftError::SmbcError(ref e) => e.cause(),
            ForkliftError::ConvertStringError(ref c) => match c {
                ConvertStringError::FromUtf16Error(ref e) => e.cause(),
                ConvertStringError::FromUtf8Error(ref e) => e.cause(),
                ConvertStringError::StringParseError(ref e) => e.cause(),
            },
            ForkliftError::IpLocalError(ref _d) => None,
            ForkliftError::InvalidConfigError(ref _d) => None,
            ForkliftError::FSError(ref _d) => None,
            ForkliftError::RecvError(ref e) => e.cause(),
            ForkliftError::SerdeJsonError(ref e) => e.cause(),
            ForkliftError::ChecksumError(ref _d) => None,
            ForkliftError::PostgresError(ref e) => e.cause(),
            ForkliftError::CrossbeamChannelError(ref _d) => None,
            ForkliftError::TimeoutError(ref _d) => None,
        }
    }
}

impl From<IoError> for ForkliftError {
    fn from(err: IoError) -> ForkliftError {
        ForkliftError::IoError(err)
    }
}

impl From<SystemTimeError> for ForkliftError {
    fn from(err: SystemTimeError) -> ForkliftError {
        ForkliftError::SystemTimeError(err)
    }
}

impl From<NanomsgError> for ForkliftError {
    fn from(err: NanomsgError) -> ForkliftError {
        ForkliftError::NanomsgError(err)
    }
}

impl From<AddrParseError> for ForkliftError {
    fn from(err: AddrParseError) -> ForkliftError {
        ForkliftError::AddrParseError(err)
    }
}

impl From<SmbcError> for ForkliftError {
    fn from(err: SmbcError) -> ForkliftError {
        ForkliftError::SmbcError(err)
    }
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

impl From<RecvError> for ForkliftError {
    fn from(err: RecvError) -> ForkliftError {
        ForkliftError::RecvError(err)
    }
}

impl From<SerdeJsonError> for ForkliftError {
    fn from(err: SerdeJsonError) -> ForkliftError {
        ForkliftError::SerdeJsonError(err)
    }
}

impl From<PostgresError> for ForkliftError {
    fn from(err: PostgresError) -> ForkliftError {
        ForkliftError::PostgresError(err)
    }
}
