extern crate api;
extern crate flatbuffers;

use self::api::service_generated::*;

#[test]
fn test_create_message() {
    let expected_result: Vec<u8> = vec![
        // Offset to root table
        12, 0, 0, 0, // Size of table
        8, 0, 12, 0, 7, 0, 8, 0, 8, 0, 0, 0, 0, 0, 0, // MessageType?
        2, 4, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, // u32 Element Count
        16, 0, 0, 0, // Ip Address String
        49, 57, 50, 46, 49, 54, 56, 46, 49, 46, 49, 58, // Port String + Nul
        53, 50, 53, 48, 0, // ??
        0, 0, 0,
    ];

    let result = create_message(
        MessageType::HEARTBEAT,
        &vec!["192.168.1.1:5250".to_string()],
    );
    println!("{:?}", result);
    assert_eq!(result, expected_result);

    let expected_result: Vec<u8> = vec![
        12, 0, 0, 0, 8, 0, 8, 0, 0, 0, 4, 0, 8, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 16, 0,
        0, 0, 49, 57, 50, 46, 49, 54, 56, 46, 49, 46, 49, 58, 53, 50, 53, 48, 0, 0, 0, 0,
    ];
    let result = create_message(MessageType::GETLIST, &vec!["192.168.1.1:5250".to_string()]);
    println!("{:?}", result);
    assert_eq!(result, expected_result);

    let expected_result: Vec<u8> = vec![
        12, 0, 0, 0, 8, 0, 12, 0, 7, 0, 8, 0, 8, 0, 0, 0, 0, 0, 0, 1, 4, 0, 0, 0, 3, 0, 0, 0, 12,
        0, 0, 0, 32, 0, 0, 0, 52, 0, 0, 0, 16, 0, 0, 0, 49, 57, 50, 46, 49, 54, 56, 46, 49, 46, 49,
        58, 53, 50, 53, 48, 0, 0, 0, 0, 16, 0, 0, 0, 49, 55, 50, 46, 49, 49, 49, 46, 50, 46, 50,
        58, 53, 53, 53, 53, 0, 0, 0, 0, 14, 0, 0, 0, 55, 50, 46, 49, 50, 46, 56, 46, 56, 58, 56,
        48, 56, 48, 0, 0,
    ];
    let result = create_message(
        MessageType::NODELIST,
        &vec![
            "192.168.1.1:5250".to_string(),
            "172.111.2.2:5555".to_string(),
            "72.12.8.8:8080".to_string(),
        ],
    );
    println!("{:?}", result);
    assert_eq!(result, expected_result);
}

#[test]
fn test_read_message() {
    let input = &vec![
        // Offset to root table
        12, 0, 0, 0, // Size of table
        8, 0, 12, 0, 7, 0, 8, 0, 8, 0, 0, 0, 0, 0, 0, // MessageType?
        2, 4, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 16, 0, 0, // MessageType?
        0, // Ip Address
        49, 57, 50, 46, 49, 54, 56, 46, 49, 46, 49, 58, // Port
        53, 50, 53, 48, // trailing stuff?
        0, 0, 0, 0,
    ];
    let result = read_message(input);
    println!("{:?}", result);
    assert_eq!(result, Some(vec!["192.168.1.1:5250".to_string()]));

    let input = &vec![
        12, 0, 0, 0, 
        8, 0, 8, 0, 0, 0, 4, 0, 8, 0, 0, 0, 4, 0, 0, 0, 
        1, 
        0, 0, 0, 4, 0, 0, 0, 16, 0, 0, 
        0, 
        49, 57, 50, 46, 49, 54, 56, 46, 49, 46, 49, 58, 
        53, 50, 53, 48, 
        0, 0, 0, 0,
    ];
    let result = read_message(input);
    println!("{:?}", result);
    assert_eq!(result, Some(vec!["192.168.1.1:5250".to_string()]));

    let input = &vec![
        12, 0, 0, 0, 
        8, 0, 12, 0, 7, 0, 8, 0, 8, 0, 0, 0, 0, 0, 0, 1, 4, 0, 0, 0, 3, 0, 0, 0, 12,
        0, 0, 0, 32, 0, 0, 0, 52, 0, 0, 0, 16, 0, 0, 0, 
        49, 57, 50, 46, 49, 54, 56, 46, 49, 46, 49, 58, 
        53, 50, 53, 48, 
        0, 0, 0, 0, 
        16, 0, 0, 0, 
        49, 55, 50, 46, 49, 49, 49, 46, 50, 46, 50, 58, 
        53, 53, 53, 53, 
        0, 0, 0, 0, 
        14, 0, 0, 0, 
        55, 50, 46, 49, 50, 46, 56, 46, 56, 58, 56, 48, 
        56, 48, 0, 0,
    ];
    let result = read_message(input);
    println!("{:?}", result);
    assert_eq!(
        result,
        Some(vec![
            "192.168.1.1:5250".to_string(),
            "172.111.2.2:5555".to_string(),
            "72.12.8.8:8080".to_string()
        ])
    )
}

#[test]
fn test_get_message_type() {
    let expected_result = MessageType::HEARTBEAT;
    let input = &vec![
        // Offset to root table
        12, 0, 0, 0, // Size of table
        8, 0, 12, 0, 7, 0, 8, 0, 8, 0, 0, 0, 0, 0, 0, // MessageType?
        2, 4, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 16, 0, 0, // MessageType?
        0, // Ip Address
        49, 57, 50, 46, 49, 54, 56, 46, 49, 46, 49, 58, // Port
        53, 50, 53, 48, // trailing stuff?
        0, 0, 0, 0,
    ];
    let result = get_message_type(input);
    println!("{:?}", result);
    assert_eq!(result, expected_result);

    let expected_result = MessageType::GETLIST;
    let input = &vec![
        12, 0, 0, 0, 8, 0, 8, 0, 0, 0, 4, 0, 8, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 16, 0,
        0, 0, 49, 57, 50, 46, 49, 54, 56, 46, 49, 46, 49, 58, 53, 50, 53, 48, 0, 0, 0, 0,
    ];
    let result = get_message_type(input);
    println!("{:?}", result);
    assert_eq!(result, expected_result);

    let expected_result = MessageType::NODELIST;
    let input = &vec![
        12, 0, 0, 0, 8, 0, 12, 0, 7, 0, 8, 0, 8, 0, 0, 0, 0, 0, 0, 1, 4, 0, 0, 0, 3, 0, 0, 0, 12,
        0, 0, 0, 32, 0, 0, 0, 52, 0, 0, 0, 16, 0, 0, 0, 49, 57, 50, 46, 49, 54, 56, 46, 49, 46, 49,
        58, 53, 50, 53, 48, 0, 0, 0, 0, 16, 0, 0, 0, 49, 55, 50, 46, 49, 49, 49, 46, 50, 46, 50,
        58, 53, 53, 53, 53, 0, 0, 0, 0, 14, 0, 0, 0, 55, 50, 46, 49, 50, 46, 56, 46, 56, 58, 56,
        48, 56, 48, 0, 0,
    ];
    let result = get_message_type(input);
    println!("{:?}", result);
    assert_eq!(result, expected_result);
}

pub fn create_message(m_type: MessageType, message: &[String]) -> Vec<u8> {
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let v: Vec<&str> = message.iter().map(|x| &**x).collect();
    let nodes = builder.create_vector_of_strings(&v);

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
                i += 1;
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
