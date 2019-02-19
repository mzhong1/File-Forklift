use api;

use crossbeam::channel::{Sender, Receiver};
use log::*;
use nanomsg::{Error, PollFd, PollInOut, PollRequest, Socket};
use std::net::SocketAddr;

use self::api::service_generated::*;
use crate::error::*;
use crate::message;
use crate::node::*;
use crate::postgres_logger::{send_mess, LogMessage};
use crate::pulse::*;
use crate::socket_node::*;
use crate::tables::*;
use crate::EndState;

pub struct Cluster {
    pub lifetime: u64,
    pub pulse: Pulse,
    pub names: NodeList,
    pub nodes: NodeMap,
    pub router: Socket,
    pub sender: Sender<ChangeList>,
    pub send_log: Sender<LogMessage>,
}

impl Cluster {
    pub fn new(
        lifetime: u64,
        router: Socket,
        init: &SocketAddr,
        sender: Sender<ChangeList>,
        send_log: Sender<LogMessage>,
    ) -> Self {
        let mut names = NodeList::new();
        names.add_node_to_list(init);
        Cluster {
            lifetime,
            pulse: Pulse::new(1000),
            names,
            nodes: NodeMap::new(),
            router,
            sender,
            send_log,
        }
    }

    fn is_valid_cluster(&self) -> (bool, String) {
        if self.lifetime == 0 {
            return (false, "Lifetime is 0!".to_string());
        }
        if self.names.node_list.is_empty() {
            return (false, "Name list is Empty!".to_string());
        }
        (true, "Cluster is Valid".to_string())
    }

    /**
     * connect_node: &self * &str -> ForkliftResult<()>
     * REQUIRES: router in self is a valid Socket
     * ENSURES: connects router to the address of full_address, output
     * error otherwise
     */
    pub fn connect_node(&mut self, full_address: &SocketAddr) -> ForkliftResult<()> {
        debug!("Try to connect router to {}", full_address);
        let tcp: String = format!("tcp://{}", full_address.to_string());
        self.router.connect(&tcp)?;
        Ok(())
    }

    /**
     * add_node: &mut self * &str * bool-> null
     * REQUIRES: router in self is properly connected
     * ENSURES: makes a new node given that the node names does not previously exist, and adds itself to both the
     * node_Names and the nodes, and connects the node to given.  Otherwise it does nothing.
     */
    pub fn add_node(&mut self, full_address: &SocketAddr, heartbeat: bool) -> ForkliftResult<()> {
        if !self.names.contains_full_address(full_address) {
            debug!("Node names before adding {:?}", self.names.node_list);
            debug!("Node Map before adding {:?}", self.nodes.node_map);
            self.names.add_node_to_list(&full_address);
            self.nodes
                .add_node_to_map(&full_address, self.lifetime, heartbeat)?;
            match self.connect_node(&full_address) {
                Ok(t) => t,
                Err(e) => {
                    error!("Unable to connect to the node at ip address: {}", e);
                    self.send_message(LogMessage::Error(e))?;
                }
            };
            debug!("Node names after adding {:?}", self.names.node_list);
            debug!("Node Map after adding {:?}", self.nodes.node_map);
        }
        Ok(())
    }

    /**
     * send_getlist: &self * &str * &PollRequest -> ForkliftResult<()>
     * REQUIRES: &PollRequest a valid file descriptor
     * full_addr in the form of ip:port, router in self a valid socket
     * ENSURES: sends a GETLIST with the message of full_address to the cluster.
     */
    pub fn send_getlist(
        &mut self,
        full_address: &SocketAddr,
        request: &PollRequest<'_>,
    ) -> ForkliftResult<()> {
        let beat = self.pulse.beat();
        if request.get_fds()[0].can_write() && beat {
            debug!("Send a GETLIST from {}", full_address);
            let message =
                message::create_message(MessageType::GETLIST, &[full_address.to_string()]);
            match self.router.nb_write(message.as_slice()) {
                Ok(_) => debug!("GETLIST sent from {}", full_address),
                Err(Error::TryAgain) => {
                    self.send_message(LogMessage::ErrorType(
                        ErrorType::NanomsgError,
                        "Receiver not ready, message can't be sent".to_string(),
                    ))?;
                }
                Err(e) => {
                    self.send_message(LogMessage::ErrorType(
                        ErrorType::NanomsgError,
                        format!("Error {:?}, Failed to write to socket", e),
                    ))?;
                }
            };
        }
        Ok(())
    }
    pub fn send_message(&self, mess: LogMessage) -> ForkliftResult<()> {
        send_mess(mess, &self.send_log)?;
        Ok(())
    }
    /**
     * send_nodelist: &mut self * &[String] -> null
     * REQUIRES: self is valid, msg_body should be non-empty, with the first and only item in the message body
     * from the GETLIST recieved message body (containing the address of the node asking for the NODELIST),
     * router a valid connected socket
     * ENSURES: The router sends a NODELIST to the sender of a GETLIST request (although it goes to all connected nodes),
     * otherwise it does nothing if the message body of the GETLIST request is empty.  
     */
    pub fn send_nodelist(&mut self, msg_body: &[String]) -> ForkliftResult<()> {
        let address_names = self.names.to_string_vector();
        let buffer = message::create_message(MessageType::NODELIST, &address_names);
        let (valid, err) = self.is_valid_cluster();
        if !valid {
            // self.conn send error here
            return Err(ForkliftError::HeartbeatError(format!(
                "Error {:?}, cluster invalid",
                err,
            )));
        }
        if !msg_body.is_empty() {
            match &msg_body[0].parse::<SocketAddr>() {
                Ok(s) => {
                    self.add_node(&s, true)?;
                    debug!("Send a NODELIST to {:?}", s);
                    match self.router.nb_write(buffer.as_slice()) {
                        Ok(_) => debug!("NODELIST sent to {:?}!", s),
                        Err(Error::TryAgain) => {
                            self.send_message(LogMessage::ErrorType(
                                ErrorType::NanomsgError,
                                "Receiver not ready, message can't be sent".to_string(),
                            ))?;
                        }
                        Err(err) => {
                            self.send_message(LogMessage::ErrorType(
                                ErrorType::NanomsgError,
                                format!("Error {:?}. Problem while writing", err),
                            ))?;
                        }
                    };
                }
                Err(e) => {
                    error!(
                        "Unable to parse the sender's address into a SocketAddr {}",
                        e
                    );
                    self.send_message(LogMessage::ErrorType(
                        ErrorType::AddrParseError,
                        format!("Error {:?}, unable to parse the sender's address", e),
                    ))?;
                }
            };
        }
        Ok(())
    }

    /**
     * send_heartbeat: &mut self * &str -> null
     * REQUIRES: router in self a valid Socket
     * ENSURES: sends a HEARTBEAT message to all connected nodes
     */
    pub fn send_heartbeat(&mut self, full_address: &SocketAddr) -> ForkliftResult<()> {
        debug!("Send a HEARTBEAT!");
        let buffer = vec![full_address.to_string()];
        let msg = message::create_message(MessageType::HEARTBEAT, &buffer);
        match self.router.nb_write(msg.as_slice()) {
            Ok(_) => debug!("HEARTBEAT sent!"),
            Err(Error::TryAgain) => {
                self.send_message(LogMessage::ErrorType(
                    ErrorType::NanomsgError,
                    "Receiver not ready, message can't be sent".to_string(),
                ))?;
            }
            Err(err) => {
                self.send_message(LogMessage::ErrorType(
                    ErrorType::NanomsgError,
                    format!("Error {:?}. Problem while writing", err),
                ))?;
            }
        };
        Ok(())
    }

    /**
     * tickdown_nodes: &mut self -> null
     * REQUIRES: is valid Cluster
     * ENSURES: for all nodes that have not sent a HEARTBEAT message to you within
     * a second, tickdown their liveness.  For all nodes that HAVE sent you a
     * HEARTBEAT message, reset their has_heartbeat value to false
     */
    pub fn tickdown_nodes(&mut self) -> ForkliftResult<()> {
        let (valid, err) = self.is_valid_cluster();
        if !valid {
            return Err(ForkliftError::HeartbeatError(format!(
                "Error {:?}, cluster invalid",
                err
            )));
        }
        trace!("Tickdown and reset nodes");
        for name in &self.names.to_string_vector() {
            let s = &self.sender;
            let log = &self.send_log;
            self.nodes.node_map.entry(name.to_string()).and_modify(|n| {
                if !n.has_heartbeat && n.tickdown() {
                    let cl = ChangeList::new(ChangeType::RemNode, SocketNode::new(n.name));
                    if s.send(cl).is_err() {
                        send_mess(
                            LogMessage::ErrorType(
                                ErrorType::CrossbeamChannelError,
                                "Channel to rendezvous is broken".to_string(),
                            ),
                            log,
                        )
                        .unwrap();
                        panic!("Channel to rendezvous is broken!");
                    }
                } else {
                    n.has_heartbeat = false;
                    debug!("HEARTBEAT was heard for node {:?}", n);
                }
            });
        }
        Ok(())
    }

    /**
     * send_and_tickdown: &PollRequest * &mut u64 * &str * &mut Socket * u64 * &mut HashMap<String, Node> * &mut Vec<SocketAddr> -> ForkliftRequest<()>
     * REQUIRES: request is a valid vector of PollRequests, router a valid Socket,
     * is valid Cluster
     * ENSURES: returns Ok(()) if successfully sending a heartbeat to connected nodes and ticking down,
     * otherwise return Err
     */
    pub fn send_and_tickdown(
        &mut self,
        full_address: &SocketAddr,
        request: &PollRequest<'_>,
    ) -> ForkliftResult<()> {
        let (valid, err) = self.is_valid_cluster();
        if !valid {
            return Err(ForkliftError::HeartbeatError(format!(
                "Error {:?}, cluster invalid",
                err
            )));
        }
        if request.get_fds()[0].can_write() {
            let beat = self.pulse.beat();
            if beat {
                self.send_heartbeat(full_address)?;
                self.tickdown_nodes()?;
            }
        }
        Ok(())
    }

    /**
     * read_message_to_u8: &self-> Vec<u8>
     * REQUIRES: router in self a valid working socket
     * ENSURES: returns the next message queued to the router as a Vec<u8>
     */
    pub fn read_message_to_u8(&mut self) -> ForkliftResult<Vec<u8>> {
        let mut buffer = Vec::new();
        match self.router.nb_read_to_end(&mut buffer) {
            Ok(_) => debug!("Read message {} bytes!", buffer.len()),
            Err(Error::TryAgain) => {
                self.send_message(LogMessage::ErrorType(
                    ErrorType::NanomsgError,
                    "Nothing to be read".to_string(),
                ))?;
            }
            Err(err) => {
                self.send_message(LogMessage::ErrorType(
                    ErrorType::NanomsgError,
                    format!("Error {:?}. Problem while writing", err),
                ))?;
            }
        };
        Ok(buffer)
    }

    /**
     * parse_nodelist_message: &self * &mut bool * &[u8] -> null
     * REQUIRES: buf a valid message read from the socket, is valid cluster
     * router in self a valid Socket, has_nodelist is false
     * ENSURES: parses a NODELIST message into a node_list and creates/adds the nodes received to the cluster
     */
    pub fn parse_nodelist_message(
        &mut self,
        has_nodelist: &mut bool,
        buf: &[u8],
    ) -> ForkliftResult<()> {
        let (valid, err) = self.is_valid_cluster();
        if !valid {
            return Err(ForkliftError::HeartbeatError(format!(
                "Error {:?}, cluster invalid",
                err
            )));
        }
        let mut tossed = false;
        if !*has_nodelist {
            debug!("Parse the NODELIST!");
            let list = match message::read_message(buf) {
                Some(t) => t,
                None => {
                    self.send_message(LogMessage::ErrorType(
                        ErrorType::HeartbeatError,
                        "NODELIST message is empty".to_string(),
                    ))?;
                    vec![]
                }
            };
            for l in &list {
                match l.parse::<SocketAddr>() {
                    Ok(s) => {
                        self.add_node(&s, false)?;
                    }
                    Err(e) => {
                        self.send_message(LogMessage::ErrorType(
                            ErrorType::AddrParseError,
                            format!("Error {:?}, unable to parse socket address {:?}", e, l),
                        ))?;
                        tossed = true
                    }
                };
            }
            if !list.is_empty() && !tossed {
                *has_nodelist = true;
            }
        }
        Ok(())
    }

    /**
     * heartbeat_heard: &[String] * &mut Vec<SocketAddr> * &mut HashMap<String, Node> * i64 * &mut Socket &str -> null
     * REQUIRES: msg_body not empty, is valid cluster, router a valid Socket
     * ENSURES: updates the hashmap to either: add a new node if the heartbeart came from a new node,
     * or updates the liveness of the node the heartbeat came from
     */
    pub fn heartbeat_heard(&mut self, msg_body: &[String]) -> ForkliftResult<()> {
        let (valid, err) = self.is_valid_cluster();
        if !valid {
            return Err(ForkliftError::HeartbeatError(format!(
                "Error {:?}, cluster invalid",
                err
            )));
        }
        if !msg_body.is_empty() {
            match &msg_body[0].parse::<SocketAddr>() {
                Ok(sent_address) => {
                    self.add_node(&sent_address, true)?;
                    let s = &self.sender;
                    let log = &self.send_log;
                    self.nodes
                        .node_map
                        .entry(sent_address.to_string())
                        .and_modify(|n| {
                            if n.heartbeat() {
                                let cl = ChangeList::new(
                                    ChangeType::AddNode,
                                    SocketNode::new(*sent_address),
                                );
                                if s.send(cl).is_err() {
                                    send_mess(
                                        LogMessage::ErrorType(
                                            ErrorType::CrossbeamChannelError,
                                            "Channel to rendezvous is broken".to_string(),
                                        ),
                                        log,
                                    )
                                    .unwrap();
                                    panic!("Channel to rendezvous is broken!");
                                }
                            }
                        });
                }
                Err(e) => {
                    self.send_message(LogMessage::ErrorType(
                        ErrorType::AddrParseError,
                        format!("Error {:?}, unable to parse socket address", e),
                    ))?;
                }
            };
        }
        Ok(())
    }

    /**
     * read_and_heartbeat: &mut self * &PollRequest * &mut bool * &str -> null
     * REQUIRES: request not empty, router in self is connected, is valid cluster
     * lifetime in self > 0
     * ENSURES: reads incoming messages and sends out heartbeats every interval milliseconds.  
     */
    pub fn read_and_heartbeat(
        &mut self,
        request: &PollRequest<'_>,
        has_nodelist: &mut bool,
        full_address: &SocketAddr,
    ) -> ForkliftResult<()> {
        let (valid, err) = self.is_valid_cluster();
        if !valid {
            return Err(ForkliftError::HeartbeatError(format!(
                "Error {:?}, cluster invalid",
                err
            )));
        }
        if request.get_fds()[0].can_read() {
            //check message type
            let msg = self.read_message_to_u8()?;
            let msgtype = message::get_message_type(&msg);
            let msg_body = match message::read_message(&msg) {
                Some(t) => t,
                None => {
                    error!("Message body is empty. Ignore the message");
                    self.send_message(LogMessage::ErrorType(
                        ErrorType::HeartbeatError,
                        "Message body is empty. Ignore the message".to_string(),
                    ))?;
                    vec![]
                }
            };
            match msgtype {
                MessageType::NODELIST => {
                    debug!("Can read message of type NODELIST");
                    self.parse_nodelist_message(has_nodelist, &msg)?;
                }
                MessageType::GETLIST => {
                    debug!("Can read message of type GETLIST");
                    self.send_nodelist(&msg_body)?;
                }
                MessageType::HEARTBEAT => {
                    debug!("Can read message of type HEARTBEAT");
                    self.heartbeat_heard(&msg_body)?;
                    if !*has_nodelist {
                        match self.send_getlist(full_address, request) {
                            Ok(t) => t,
                            Err(e) => {
                                return Err(e);
                            }
                        };
                    }
                }
            }
        }
        Ok(())
    }

    /*
        if node_joined has been flagged, then we need to connect the node to the graph.
        This is done by sending a GETLIST signal to the node that we are connected to
        every second until we get a NODELIST back.
        Poll THIS machine's node
            Pollin using timeout of pulse interval
            if !has_nodelist:
                send GETLIST to connected nodes
            if can_read():
                if NODESLIST:
                    unpack message to get list of nodes,
                    update nodelist and nodes,
                    connect to list of nodes
                    set has_nodelist to true
                if GETLIST:
                    unpack message to get the sender address
                    add sender to node_names + map
                    send Nodelist to sender address
                if HEARTBEAT message from some socket
                (ip address of the heartbeat sender):
                    unpack message to find out sender
                    if the sender is not in the list of nodes, add it to the node_names
                        and the node_map and connect
                    update the liveness of the sender
                    update had_heartbeat of node to true
            if can_write()
                if SystemTime > heartbeat_at:
                    send HEARTBEAT
                    loop through nodes in map
                        if node's had_heartbeat = true
                            reset had_heartbeat to false
                        else (had_heartbeat = false)
                            if liveness <= 0
                                assume node death
                                remove node from rendezvous
    */
    pub fn heartbeat_loop(
        &mut self,
        full_address: &SocketAddr,
        has_nodelist: &mut bool,
        recv_end: &Receiver<EndState>,
    ) -> ForkliftResult<()> {
        let mut countdown = 0;
        loop {
            if recv_end.try_recv().is_ok() {
                println!("Got exit");
                break;
            }
            if countdown > 5000 && !*has_nodelist {
                return Err(ForkliftError::TimeoutError(format!(
                    "{} has not responded for a lifetime, please join to a different ip:port",
                    full_address
                )));
            }
            ::std::thread::sleep(::std::time::Duration::from_millis(10));
            countdown += 10;
            let mut items: Vec<PollFd> = vec![self.router.new_pollfd(PollInOut::InOut)];
            let mut request = PollRequest::new(&mut items);
            trace!("Attempting to poll the socket");
            Socket::poll(&mut request, self.pulse.interval as isize)?;

            if !*has_nodelist {
                match self.send_getlist(full_address, &request) {
                    Ok(t) => t,
                    Err(e) => {
                        error!("Time ran backwards!  Abort! {}", e);
                        return Err(e);
                    }
                };
            }
            self.read_and_heartbeat(&request, has_nodelist, full_address)?;
            self.send_and_tickdown(full_address, &request)?;
        }
        Ok(())
    }

    pub fn init_connect(&mut self, full_address: &SocketAddr) -> ForkliftResult<()> {
        trace!("Initializing connection...");
        for node_ip in self.names.node_list.clone() {
            if node_ip != *full_address {
                trace!("Attempting to connect to {}", node_ip);
                match self.connect_node(&node_ip) {
                    Ok(t) => t,
                    Err(e) => {
                        error!(
                            "Error: {} Unable to connect to the node at ip address: {}",
                            e, full_address
                        );
                        self.send_message(LogMessage::Error(e))?;
                    }
                };
            }
        }
        Ok(())
    }
}
