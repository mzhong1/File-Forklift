extern crate flatbuffers;

use api::service_generated::*;

pub fn create_message(
    mType: uint8,
    message: Vec<String> 
) -> &[u8]
{
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let mut nodes = builder.create_vector_of_strings(message);
    let type = match mType {
        0 => api::service_generated::MessageType::OHAI,
        1 => api::service_generated::MessageType::GETLIST,
        2 => api::service_generated::MessageType::NODELIST,
        3 => api::service_generated::MessageType::HEARTBEAT,
        _ => panic!("Invalid message type!")
    }
    let create = Message::create(
        &mut builder,
        &CreateRequestArgs {
            mtype: type;
            members: nodes;
        },
    );
    builder.finish_minimal(create);
    builder.finished_data()
}

pub fn read_message(
    buf: &[u8]
) ->
{
    let mess = get_root_as_message(buf);
    mess.members();
}

pub fn get_message_type(
    buf: &[u8]
){
    let mess = get_root_as_message(buf);
    mess.mtype();
}
