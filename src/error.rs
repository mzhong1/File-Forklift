use nanomsg::Error as NanomsgError;
use smbc::Error as SmbcError;
use std::error::Error as err;
use std::fmt;
use std::io::Error as IoError;
use std::net::AddrParseError;
use std::time::SystemTimeError;

pub type ForkliftResult<T> = Result<T, ForkliftError>;

#[derive(Debug)]
pub enum ForkliftError {
    IoError(IoError),
    SystemTimeError(SystemTimeError),
    NanomsgError(NanomsgError),
    AddrParseError(AddrParseError),
    SmbcError(SmbcError),
    IpLocalError,
    InvalidConfigError,
    FSError(String),
}

impl fmt::Display for ForkliftError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ForkliftError::InvalidConfigError => f.write_str("InvalidConfigError"),
            ForkliftError::IpLocalError => f.write_str("IpLocalError"),
            ForkliftError::FSError(d) => f.write_str("FileSystemError"),
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
            ForkliftError::IpLocalError => "Could not determine local ip address",
            ForkliftError::InvalidConfigError => "Invalid config formatting",
            ForkliftError::FSError(d) => &d,
        }
    }

    fn cause(&self) -> Option<&err> {
        match *self {
            ForkliftError::IoError(ref e) => e.cause(),
            ForkliftError::SystemTimeError(ref e) => e.cause(),
            ForkliftError::NanomsgError(ref e) => e.cause(),
            ForkliftError::AddrParseError(ref e) => e.cause(),
            ForkliftError::SmbcError(ref e) => e.cause(),
            ForkliftError::IpLocalError => None,
            ForkliftError::InvalidConfigError => None,
            ForkliftError::FSError(d) => None,
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
