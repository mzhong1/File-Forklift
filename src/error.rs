use std::error::Error as err;
use std::fmt;
use std::io::Error as IoError;
use std::time::SystemTimeError;
use nanomsg::Error as NanomsgError;
use serde_json::error::Error as JSONError;

pub type ForkliftResult<T> = Result<T, ForkliftError>;

#[derive(Debug)]
pub enum NodeError{
    AddressNotFoundError,
    IpNotFoundError,
    PortNotFoundError,
}

impl fmt::Display for NodeError{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result{
        match *self{
            NodeError::AddressNotFoundError => f.write_str("AddressNotFound"),
            NodeError::IpNotFoundError => f.write_str("IpNotFound"),
            NodeError::PortNotFoundError => f.write_str("PortNotFound"),
        }
    }
}

impl err for NodeError{
    fn description(&self) -> &str{
        match &self{
            NodeError::AddressNotFoundError => "Full Address not found",
            NodeError::IpNotFoundError => "Ip address not found",
            NodeError::PortNotFoundError => "Port number not found",
        }
    }
}

#[derive(Debug)]
pub enum ForkliftError {
    IoError(IoError),
    SystemTimeError(SystemTimeError),
    NanomsgError(NanomsgError),
    JSONError(JSONError),
    NodeNotFoundError(NodeError),
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
            ForkliftError::JSONError(ref e) => e.description(),
            ForkliftError::NodeNotFoundError(ref e) => e.description(),
        }
    }

    fn cause(&self) -> Option<&err> {
        match *self {
            ForkliftError::IoError(ref e) => e.cause(),
            ForkliftError::SystemTimeError(ref e) => e.cause(),
            ForkliftError::NanomsgError(ref e) => e.cause(),
            ForkliftError::JSONError(ref e) => e.cause(),
            ForkliftError::NodeNotFoundError(ref e) => e.cause(),
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

impl From<JSONError> for ForkliftError{
    fn from(err: JSONError) -> ForkliftError{
        ForkliftError::JSONError(err)
    }
}

impl From<NodeError> for ForkliftError{
    fn from(err: NodeError) -> ForkliftError{
        ForkliftError::NodeNotFoundError(err)
    }
}