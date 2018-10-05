extern crate api;
extern crate flatbuffers;

use self::api::service_generated::*;

pub fn create_message(m_type: MessageType, message: &Vec<String>) -> Vec<u8> {
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let v: Vec<&str> = message.iter().map(|x| &**x).collect();
    let nodes = builder.create_vector_of_strings(&v);
    /*
        let messageT = match mType {
            0 => MessageType::OHAI,
            1 => MessageType::GETLIST,
            2 => MessageType::NODELIST,
            3 => MessageType::HEARTBEAT,
            // Might want to return Err here instead of panic
            _ => panic!("Invalid message type!"),
        };
    */
    let create = Message::create(
        &mut builder,
        &MessageArgs {
            mtype: m_type,
            members: Some(nodes),
        },
    );
    builder.finish_minimal(create);
    builder.finished_data().to_vec()
}

pub fn read_message(buf: &[u8]) -> Option<Vec<String>> {
    let mess = get_root_as_message(&buf);
    mess.members().iter();
    match mess.members() {
        Some(s) => {
            let mut v: Vec<String> = vec![];
            let mut i = 0;
            while i < s.len() {
                v.push(s.get(i).to_string());
            }
            Some(v)
        }
        None => None,
    }
}

pub fn get_message_type(buf: &[u8]) -> MessageType {
    let mess = get_root_as_message(buf);
    mess.mtype()
}
