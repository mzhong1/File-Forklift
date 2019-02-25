use api;
use log::*;
use protobuf::parse_from_bytes;
use protobuf::Message as ProtobufMessage;
use protobuf::*;

use self::api::service::Message;
use self::api::service::*;
use crate::error::*;
/// create a new message to send
pub fn create_message(m_type: MessageType, message: &[String]) -> ForkliftResult<Vec<u8>> {
    trace!("Creating Message {:?} with body {:?}", m_type, message);
    let mut new_message = Message::new();
    let v = RepeatedField::from_slice(message);
    trace!("Converted message vector {:?} to protobuf vector", v);
    new_message.set_members(v);
    new_message.set_mtype(m_type);
    trace!("Set all Message fields");
    Ok(new_message.write_to_bytes()?) //handle this later
}

/// read a serialized message to a vector of string
pub fn read_message(buf: &[u8]) -> ForkliftResult<Vec<String>> {
    let op_result = parse_from_bytes::<api::service::Message>(buf)?;
    Ok(op_result.get_members().to_vec())
}

/// get the message type of a message
pub fn get_message_type(buf: &[u8]) -> ForkliftResult<MessageType> {
    let op_result = parse_from_bytes::<api::service::Message>(buf)?;
    Ok(op_result.get_mtype())
}
