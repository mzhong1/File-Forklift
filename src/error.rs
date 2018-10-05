use std::error::Error as err;
use std::fmt;
use std::io::Error as IoError;
use std::time::SystemTimeError;
use nanomsg::Error as NanomsgError;

pub type ForkliftResult<T> = Result<T, ForkliftError>;

#[derive(Debug)]
pub enum ForkliftError {
    IoError(IoError),
    SystemTimeError(SystemTimeError),
    NanomsgError(NanomsgError),
}

impl fmt::Display for ForkliftError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.description())
    }
}

impl err for ForkliftError {
    fn description(&self) -> &str {
        match *self {
            ForkliftError::IoError(ref e) => e.description(),
            ForkliftError::SystemTimeError(ref e) => e.description(),
            ForkliftError::NanomsgError(ref e) => e.description(),
        }
    }

    fn cause(&self) -> Option<&err> {
        match *self {
            ForkliftError::IoError(ref e) => e.cause(),
            ForkliftError::SystemTimeError(ref e) => e.cause(),
            ForkliftError::NanomsgError(ref e) => e.cause(),
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
