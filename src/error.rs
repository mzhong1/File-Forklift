use std::error;
use std::fmt;

#[derive(Debug)]
pub enum ForkliftError {
    InvalidMessageType(InvalidMessageType),
    InvalidMessageLength(InvalidMessageLength),
}

impl fmt::Display for ForkliftError
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result{
        f.write_str(self.description())
    }
}

impl err for ForkliftError
{
    fn description(&self) -> &str{
        match *self{
            ForkliftError::InvalidMessageLength(ref e) => e.description(),
            ForkliftError::InvalidMessageType(ref e) => e.description(),
        }
    }
}

fn cause(&self) -> Option<&err>{
    match *self{
        ForkliftError::InvalidMessageLength(ref e) => e.cause(),
        ForkliftError::InvalidMessageType(ref e) => e.cause()
    }
}