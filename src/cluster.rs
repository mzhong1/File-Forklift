use self::api::service::*;
use api;

use crossbeam::channel::{Receiver, Sender};
use log::*;
use nanomsg::{Error, PollFd, PollInOut, PollRequest, Socket};
use std::net::SocketAddr;

use crate::error::{ForkliftError, ForkliftResult};
use crate::message;
use crate::node::*;
use crate::postgres_logger::{send_mess, LogMessage};
use crate::pulse::*;
use crate::socket_node::*;
use crate::tables::ErrorType;
use crate::EndState;

/// An object representing a cluster of nodes
pub struct Cluster {
    /// the number of seconds a node can live without hearing a heartbeat
    pub lifetime: u64,
    /// Object to determine when to beat
    pub pulse: Pulse,
    /// List of socket addresses in the cluster
    pub names: NodeList,
    /// mapping of socket addresses to SocketNodes
    pub nodes: NodeMap,
    /// router used to connect with other nodes
    pub router: Socket,
    /// current node address
    pub node_address: SocketAddr,
    /// channel to rendezvous thread
    pub node_change_output: Sender<ChangeList>,
    /// channel to postgres logger
    pub log_output: Sender<LogMessage>,
}

impl Cluster {
    /// Create a new cluster, Pulse is set by default to 1000, or 1 second
    pub fn new(
        lifetime: u64,
        router: Socket,
        node_address: &SocketAddr,
        node_change_output: Sender<ChangeList>,
        log_output: Sender<LogMessage>,
    ) -> Self {
        let mut names = NodeList::new();
        names.add_node_to_list(node_address);
        Cluster {
            lifetime,
            pulse: Pulse::new(1000),
            names,
            nodes: NodeMap::new(),
            router,
            node_address: *node_address,
            node_change_output,
            log_output,
        }
    }

    /// checks if the Cluster is valid
    fn is_valid_cluster(&self) -> ForkliftResult<()> {
        if self.lifetime == 0 {
            return Err(ForkliftError::HeartbeatError(
                "Error, lifetime is 0! cluster invalid".to_string(),
            ));
        }
        if self.names.node_list.is_empty() {
            return Err(ForkliftError::HeartbeatError(
                "Error, name list is empty! cluster invalid".to_string(),
            ));
        }
        Ok(())
    }

    /// connect router to an address
    pub fn connect_node(&mut self, node_address: &SocketAddr) -> ForkliftResult<()> {
        debug!("Try to connect router to {}", node_address);
        let tcp: String = format!("tcp://{}", node_address.to_string());
        self.router.connect(&tcp)?;
        Ok(())
    }

    /// Add a new node to the cluster if it does not previously exist
    pub fn add_node(&mut self, node_address: &SocketAddr, heartbeat: bool) -> ForkliftResult<()> {
        if !self.names.contains_address(node_address) {
            debug!("Node names before adding {:?}", self.names.node_list);
            debug!("Node Map before adding {:?}", self.nodes.node_map);
            self.names.add_node_to_list(&node_address);
            self.nodes.add_node_to_map(&node_address, self.lifetime, heartbeat)?;
            match self.connect_node(&node_address) {
                Ok(t) => t,
                Err(e) => {
                    error!("Unable to connect to the node at ip address: {}", e);
                    self.send_log(LogMessage::Error(e))?;
                }
            };
            debug!("Node names after adding {:?}", self.names.node_list);
            debug!("Node Map after adding {:?}", self.nodes.node_map);
        }
        Ok(())
    }

    /// send a GETLIST message to a node with the socket address of the current node
    pub fn send_getlist(&mut self, request: &PollRequest<'_>) -> ForkliftResult<()> {
        if request.get_fds()[0].can_write() && self.pulse.beat() {
            trace!("Send a GETLIST from {}", self.node_address);
            let msg =
                message::create_message(MessageType::GETLIST, &[self.node_address.to_string()]);
            self.send_message(&msg, "Getlist sent!")?;
        }
        Ok(())
    }

    /// send message to postgres
    pub fn send_log(&self, log: LogMessage) -> ForkliftResult<()> {
        send_mess(log, &self.log_output)?;
        Ok(())
    }

    /// send a nodelist message to all connected nodes upon receiving a GETLIST request from a node.
    /// Do nothing if the GETLIST request is empty
    pub fn send_nodelist(&mut self, msg_body: &[String]) -> ForkliftResult<()> {
        let address_names = self.names.to_string_vector();
        let msg = message::create_message(MessageType::NODELIST, &address_names);
        self.is_valid_cluster()?;
        if !msg_body.is_empty() {
            match &msg_body[0].parse::<SocketAddr>() {
                Ok(s) => {
                    self.add_node(&s, true)?;
                    debug!("Send a NODELIST to {:?}", s);
                    self.send_message(&msg, "Nodelist sent!")?;
                }
                Err(e) => {
                    self.send_log(LogMessage::ErrorType(
                        ErrorType::AddrParseError,
                        format!("Error {:?}, unable to parse the sender's address", e),
                    ))?;
                }
            };
        }
        Ok(())
    }

    /// broadcast a message to the cluster
    fn send_message(&mut self, msg: &[u8], success: &str) -> ForkliftResult<()> {
        match self.router.nb_write(msg) {
            Ok(_) => debug!("{}", success),
            Err(Error::TryAgain) => {
                self.send_log(LogMessage::ErrorType(
                    ErrorType::NanomsgError,
                    "Receiver not ready, message can't be sent".to_string(),
                ))?;
            }
            Err(err) => {
                self.send_log(LogMessage::ErrorType(
                    ErrorType::NanomsgError,
                    format!("Error {:?}. Problem while writing", err),
                ))?;
            }
        };
        Ok(())
    }

    /// send a HEARTBEAT message to all connected nodes
    pub fn send_heartbeat(&mut self) -> ForkliftResult<()> {
        debug!("Send a HEARTBEAT!");
        let buffer = vec![self.node_address.to_string()];
        let msg = message::create_message(MessageType::HEARTBEAT, &buffer);
        self.send_message(&msg, "Heeartbeat sent!")?;
        Ok(())
    }

    /// tickdown the liveness of all nodes that have not sent a HEARTBEAT within a second.
    /// reset has_heartbeat to false on all nodes
    pub fn tickdown_nodes(&mut self) -> ForkliftResult<()> {
        self.is_valid_cluster()?;
        trace!("Tickdown and reset nodes");
        for name in &self.names.to_string_vector() {
            let change_output = &self.node_change_output;
            let log_output = &self.log_output;
            self.nodes.node_map.entry(name.to_string()).and_modify(|n| {
                if !n.has_heartbeat && n.tickdown() {
                    let change_list = ChangeList::new(ChangeType::RemNode, SocketNode::new(n.name));
                    if change_output.send(change_list).is_err() {
                        let mess = LogMessage::ErrorType(
                            ErrorType::CrossbeamChannelError,
                            "Channel to rendezvous is broken".to_string(),
                        );
                        send_mess(mess, log_output).unwrap();
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

    /// broadcast a heartbeat to the cluster and tick down nodes
    pub fn send_and_tickdown(&mut self, request: &PollRequest<'_>) -> ForkliftResult<()> {
        self.is_valid_cluster()?;
        if request.get_fds()[0].can_write() && self.pulse.beat() {
            self.send_heartbeat()?;
            self.tickdown_nodes()?;
        }
        Ok(())
    }

    /// get the next message queued to the router
    pub fn read_message_to_u8(&mut self) -> ForkliftResult<Vec<u8>> {
        let mut buffer = Vec::new();
        match self.router.nb_read_to_end(&mut buffer) {
            Ok(_) => debug!("Read message {} bytes!", buffer.len()),
            Err(Error::TryAgain) => {
                self.send_log(LogMessage::ErrorType(
                    ErrorType::NanomsgError,
                    "Nothing to be read".to_string(),
                ))?;
            }
            Err(err) => {
                self.send_log(LogMessage::ErrorType(
                    ErrorType::NanomsgError,
                    format!("Error {:?}. Problem while writing", err),
                ))?;
            }
        };
        Ok(buffer)
    }

    /// read serialized message to Vec<String>
    fn read_message(&self, msg: &[u8], err: &str) -> ForkliftResult<Vec<String>> {
        message::read_message(msg)
    }

    /// parse a NODELIST message into a list of nodes and create/add the nodes to the cluster
    /// @note: if has_nodelist is true, then exit without changing anything
    pub fn parse_nodelist_message(
        &mut self,
        has_nodelist: &mut bool,
        msg: &[u8],
    ) -> ForkliftResult<()> {
        self.is_valid_cluster()?;
        let mut ignored_address = false;
        if *has_nodelist {
            return Ok(());
        }
        debug!("Parse the NODELIST!");
        let node_list = self.read_message(msg, "NODELIST message is empty")?;
        for address in &node_list {
            match address.parse::<SocketAddr>() {
                Ok(node) => {
                    self.add_node(&node, false)?;
                }
                Err(e) => {
                    let mess = LogMessage::ErrorType(
                        ErrorType::AddrParseError,
                        format!("Error {:?}, unable to parse socket address {:?}", e, address),
                    );
                    self.send_log(mess)?;
                    ignored_address = true
                }
            };
        }
        if !node_list.is_empty() && !ignored_address {
            *has_nodelist = true;
        }
        Ok(())
    }

    /// updates the hashmap to either add a new node if the heartbeat came from a new node,
    /// or updates the liveness of the node
    pub fn heartbeat_heard(&mut self, msg_body: &[String]) -> ForkliftResult<()> {
        self.is_valid_cluster()?;
        if msg_body.is_empty() {
            return Ok(());
        }
        match &msg_body[0].parse::<SocketAddr>() {
            Ok(sent_address) => {
                self.add_node(&sent_address, true)?;
                let node_change_output = &self.node_change_output;
                let log_output = &self.log_output;
                self.nodes.node_map.entry(sent_address.to_string()).and_modify(|n| {
                    let change_list =
                        ChangeList::new(ChangeType::AddNode, SocketNode::new(*sent_address));
                    if n.heartbeat() && node_change_output.send(change_list).is_err() {
                        let mess = LogMessage::ErrorType(
                            ErrorType::CrossbeamChannelError,
                            "Channel to rendezvous is broken".to_string(),
                        );
                        send_mess(mess, log_output).unwrap();
                        panic!("Channel to rendezvous is broken!");
                    }
                });
            }
            Err(e) => {
                self.send_log(LogMessage::ErrorType(
                    ErrorType::AddrParseError,
                    format!("Error {:?}, unable to parse socket address", e),
                ))?;
            }
        };

        Ok(())
    }

    /// Read incoming messages and send out heartbeats every interval milliseconds.
    pub fn read_and_heartbeat(
        &mut self,
        request: &PollRequest<'_>,
        has_nodelist: &mut bool,
    ) -> ForkliftResult<()> {
        self.is_valid_cluster()?;
        if request.get_fds()[0].can_read() {
            //check message type
            let msg = self.read_message_to_u8()?;
            let msgtype = message::get_message_type(&msg)?;
            let msg_body = self.read_message(&msg, "Message body is empty, Ifnore the message")?;
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
                        self.send_getlist(request)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// run the heartbeat protocol.  
    /// This Protocol will poll the current node's socket every interval for messages and handle them as such:
    /// if !has_nodelist: send GETLIST to connected nodes
    /// if can_read()
    ///     => NODESLIST: get list of nodes, update cluster, set has_nodelist to true
    ///     => GETLIST: get the sender address, add sender to cluster, send Nodelist to sender address
    ///     => HEARTBEAT: get the sender, add sender to cluster if necessary, update the liveness of the sender,
    ///                   set had_heartbeat of node to true
    /// if can_write()
    ///     if SystemTime > heartbeat_at: send HEARTBEAT, loop through nodes in map;
    ///         if node's had_heartbeat = true: reset had_heartbeat to false
    ///         else (had_heartbeat = false)
    ///     if liveness <= 0: assume node death and remove node from rendezvous
    ///
    /// @note if the nodelist is length 2 (current node and another) query the other node
    /// with GETLIST for a NODELIST of all nodes in the cluster
    pub fn heartbeat_loop(
        &mut self,
        has_nodelist: &mut bool,
        end_heartbeat_input: &Receiver<EndState>,
    ) -> ForkliftResult<()> {
        let mut countdown = 0;
        loop {
            if end_heartbeat_input.try_recv().is_ok() {
                println!("Got exit");
                break;
            }
            if countdown > 5000 && !*has_nodelist {
                return Err(ForkliftError::TimeoutError(format!(
                    "{} has not responded for a lifetime, please join to a different ip:port",
                    self.node_address
                )));
            }
            ::std::thread::sleep(::std::time::Duration::from_millis(10));
            countdown += 10;
            let mut items: Vec<PollFd> = vec![self.router.new_pollfd(PollInOut::InOut)];
            let mut request = PollRequest::new(&mut items);
            trace!("Attempting to poll the socket");
            Socket::poll(&mut request, self.pulse.interval as isize)?;

            if !*has_nodelist {
                self.send_getlist(&request)?;
            }
            self.read_and_heartbeat(&request, has_nodelist)?;
            self.send_and_tickdown(&request)?;
        }
        Ok(())
    }

    /// initialize connections with the cluster
    pub fn init_connect(&mut self) -> ForkliftResult<()> {
        trace!("Initializing connection...");
        for node_ip in self.names.node_list.clone() {
            if node_ip != self.node_address {
                trace!("Attempting to connect to {}", node_ip);
                match self.connect_node(&node_ip) {
                    Ok(t) => t,
                    Err(e) => {
                        error!(
                            "Error: {} Unable to connect to the node at ip address: {}",
                            e, self.node_address
                        );
                        self.send_log(LogMessage::Error(e))?;
                    }
                };
            }
        }
        Ok(())
    }
}
