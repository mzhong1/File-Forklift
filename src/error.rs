use std::error::Error as err;
use std::fmt;
use std::io::Error as IoError;
use std::net::AddrParseError;
use std::time::SystemTimeError;
use nanomsg::Error as NanomsgError;

pub type ForkliftResult<T> = Result<T, ForkliftError>;

#[derive(Debug)]
pub enum ForkliftError {
    IoError(IoError),
    SystemTimeError(SystemTimeError),
    NanomsgError(NanomsgError),
    AddrParseError(AddrParseError),
    IpLocalError,
    InvalidConfigError,
}

impl fmt::Display for ForkliftError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ForkliftError::InvalidConfigError => f.write_str("InvalidConfigError"),
            ForkliftError::IpLocalError => f.write_str("IpLocalError"),
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
            ForkliftError::IpLocalError => "Could not determine local ip address",
            ForkliftError::InvalidConfigError => "Invalid config formatting",
        }
    }

    fn cause(&self) -> Option<&err> {
        match *self {
            ForkliftError::IoError(ref e) => e.cause(),
            ForkliftError::SystemTimeError(ref e) => e.cause(),
            ForkliftError::NanomsgError(ref e) => e.cause(),
            ForkliftError::AddrParseError(ref e) => e.cause(),
            ForkliftError::IpLocalError => None,
            ForkliftError::InvalidConfigError => None,
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

impl From<AddrParseError> for ForkliftError{
    fn from(err: AddrParseError) -> ForkliftError{
        ForkliftError::AddrParseError(err)
    }
}
