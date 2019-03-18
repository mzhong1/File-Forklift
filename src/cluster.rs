use self::api::service::*;
use api;

use crossbeam::channel::{Receiver, Sender, TryRecvError};
use log::*;
use nng::{Aio, ErrorKind, Message as NanoMessage, Socket};
use std::net::SocketAddr;

use crate::error::{ForkliftError, ForkliftResult};
use crate::message;
use crate::node::*;
use crate::postgres_logger::{send_mess, LogMessage};
use crate::pulse::*;
use crate::socket_node::*;
use crate::tables::{ErrorType, NodeStatus};
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
    /// whether the program will automaticall rerun or not
    pub rerun: bool,
}

impl Cluster {
    /// Create a new cluster, Pulse is set by default to 1000, or 1 second
    pub fn new(
        lifetime: u64,
        router: Socket,
        node_address: &SocketAddr,
        node_change_output: Sender<ChangeList>,
        log_output: Sender<LogMessage>,
        rerun: bool,
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
            rerun,
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
        self.router.dial(&tcp)?;
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
    pub fn send_getlist(&mut self) -> ForkliftResult<()> {
        if self.pulse.beat() {
            trace!("Send a GETLIST from {}", self.node_address);
            let msg = message::create_message(
                MessageType::GETLIST,
                &[self.node_address.to_string()],
                self.rerun,
            )?;
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
        let msg = message::create_message(MessageType::NODELIST, &address_names, self.rerun)?;
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
        let message = NanoMessage::try_from(msg)?;
        match self.router.send(message) {
            Ok(_) => debug!("{}", success),
            Err((_, e)) => match e.kind() {
                ErrorKind::TryAgain => {
                    self.send_log(LogMessage::ErrorType(
                        ErrorType::NanomsgError,
                        "Receiver not ready, message can't be sent".to_string(),
                    ))?;
                }
                _ => {
                    self.send_log(LogMessage::ErrorType(
                        ErrorType::NanomsgError,
                        format!("Error {:?}. Problem while writing", e),
                    ))?;
                }
            },
        };
        Ok(())
    }

    /// send a HEARTBEAT message to all connected nodes
    pub fn send_heartbeat(&mut self) -> ForkliftResult<()> {
        debug!("Send a HEARTBEAT!");
        let buffer = vec![self.node_address.to_string()];
        let msg = message::create_message(MessageType::HEARTBEAT, &buffer, self.rerun)?;
        self.send_message(&msg, "Heeartbeat sent!")?;
        Ok(())
    }

    /// tickdown the liveness of all nodes that have not sent a HEARTBEAT within a second.
    /// reset has_heartbeat to false on all nodes
    pub fn tickdown_nodes(&mut self) -> ForkliftResult<()> {
        self.is_valid_cluster()?;
        trace!("Tickdown and reset nodes");
        for name in &self.names.node_list {
            let change_output = &self.node_change_output;
            let log_output = &self.log_output;
            self.nodes.node_map.entry(*name).and_modify(|n| {
                if !n.has_heartbeat && n.tickdown() {
                    let change_list = ChangeList::new(ChangeType::RemNode, SocketNode::new(n.name));
                    if change_output.send(change_list).is_err() {
                        let mess = LogMessage::ErrorType(
                            ErrorType::CrossbeamChannelError,
                            "Channel to rendezvous is broken".to_string(),
                        );
                        send_mess(mess, log_output).expect("Channel to rendezvous is broken!");
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
    pub fn send_and_tickdown(&mut self) -> ForkliftResult<()> {
        self.is_valid_cluster()?;
        if self.pulse.beat() {
            self.send_heartbeat()?;
            self.tickdown_nodes()?;
        }
        Ok(())
    }

    /// get the next message queued to the router
    pub fn read_message_to_u8(&mut self, aio: &mut Aio) -> ForkliftResult<NanoMessage> {
        trace!("Attempting to read message");
        aio.wait();
        let mess = match aio.get_msg() {
            Some(m) => m,
            None => NanoMessage::new()?,
        };
        Ok(mess)
    }

    /// parse a NODELIST message into a list of nodes and create/add the nodes to the cluster
    pub fn parse_nodelist_message(
        &mut self,
        has_nodelist: &mut bool,
        msg: &[u8],
    ) -> ForkliftResult<()> {
        self.is_valid_cluster()?;
        let mut ignored_address = false;
        debug!("Parse the NODELIST!");
        let node_list = message::read_message(msg)?;
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
                self.nodes.node_map.entry(*sent_address).and_modify(|n| {
                    let change_list =
                        ChangeList::new(ChangeType::AddNode, SocketNode::new(*sent_address));
                    if n.heartbeat() && node_change_output.send(change_list).is_err() {
                        let mess = LogMessage::ErrorType(
                            ErrorType::CrossbeamChannelError,
                            "Channel to rendezvous is broken".to_string(),
                        );
                        send_mess(mess, log_output).expect("Channel to rendezvous is broken!");
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

    /// updates the hashmap to end a node
    pub fn node_finished(&mut self, msg_body: &[String]) -> ForkliftResult<()> {
        self.is_valid_cluster()?;
        if msg_body.is_empty() {
            return Ok(());
        }
        match &msg_body[0].parse::<SocketAddr>() {
            Ok(sent_address) => {
                let node_change_output = &self.node_change_output;
                let log_output = &self.log_output;
                self.nodes.node_map.entry(*sent_address).and_modify(|n| {
                    let change_list =
                        ChangeList::new(ChangeType::RemNode, SocketNode::new(*sent_address));
                    n.node_status = NodeStatus::NodeFinished;
                    if node_change_output.send(change_list).is_err() {
                        let mess = LogMessage::ErrorType(
                            ErrorType::CrossbeamChannelError,
                            "Channel to rendezvous is broken".to_string(),
                        );
                        send_mess(mess, log_output).expect("Channel to rendezvous is broken!");
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
        aio: &mut Aio,
        has_nodelist: &mut bool,
    ) -> ForkliftResult<()> {
        self.is_valid_cluster()?;
        //check message type
        match self.router.recv_async(&aio) {
            Ok(_) => {
                let msg = self.read_message_to_u8(aio)?;
                if !msg.is_empty() {
                    let (msgtype, rerun) = if msg.len() == 0 {
                        (message::get_message_type(&Vec::new())?, message::get_rerun(&Vec::new())?)
                    } else {
                        (message::get_message_type(&msg)?, message::get_rerun(&msg)?)
                    };
                    if rerun {
                        self.rerun = rerun;
                    }
                    let msg_body = message::read_message(&msg)?;
                    match msgtype {
                        MessageType::NODELIST => {
                            debug!("Can read message of type NODELIST");
                            self.parse_nodelist_message(has_nodelist, &msg)?;
                        }
                        MessageType::GETLIST => {
                            debug!("Can read message of type GETLIST");
                            self.send_nodelist(&msg_body)?;
                            *has_nodelist = false;
                        }
                        MessageType::HEARTBEAT => {
                            debug!("Can read message of type HEARTBEAT");
                            self.heartbeat_heard(&msg_body)?;
                            if !*has_nodelist {
                                self.send_getlist()?;
                            }
                        }
                        MessageType::NODEFINISHED => {
                            debug!("Can read a message of type NODEFINISHED");
                            self.node_finished(&msg_body)?;
                        }
                    }
                }
            }
            Err(e) => match e.kind() {
                ErrorKind::TryAgain => {
                    self.send_log(LogMessage::ErrorType(
                        ErrorType::NanomsgError,
                        "Nothing to be read".to_string(),
                    ))?;
                }
                _ => {
                    self.send_log(LogMessage::ErrorType(
                        ErrorType::NanomsgError,
                        format!("Error {:?}. Problem while writing", e),
                    ))?;
                }
            },
        }

        Ok(())
    }

    pub fn rerun(&self) -> Option<bool> {
        let mut died = false;
        let mut ended = true;
        for (_, node) in self.nodes.node_map.iter() {
            match node.node_status {
                NodeStatus::NodeAlive => {
                    ended = false;
                }
                NodeStatus::NodeDied => {
                    died = true;
                }
                NodeStatus::NodeFinished => (),
            }
        }
        if !self.rerun {
            Some(false)
        } else if !ended {
            None
        } else {
            Some(died)
        }
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
        check_rerun: Receiver<EndState>,
        send_rerun: Sender<EndState>,
    ) -> ForkliftResult<()> {
        let mut countdown = 0;
        let mut aio = Aio::new().unwrap();
        let mut responded = false;
        aio.set_timeout(Some(self.pulse.timeout));
        let mut check_if_rerun = false;
        loop {
            match check_rerun.try_recv() {
                Ok(EndState::EndProgram) => {
                    check_if_rerun = true;
                } //check if rerunable and send
                Ok(_) => {
                    error!("False EndState");
                    return Err(ForkliftError::HeartbeatError("False EndState".to_string()));
                } //this shouldn't happen ever
                Err(TryRecvError::Empty) => (),
                Err(_) => {
                    println!("Channel to progress worker broken!");
                    return Err(ForkliftError::CrossbeamChannelError(
                        "Channel to progress worker broken".to_string(),
                    ));
                }
            }
            if check_if_rerun {
                match self.rerun() {
                    None => (), //not finished,
                    Some(true) => {
                        send_rerun
                            .send(EndState::Rerun)
                            .expect("Channel to progress worker broken!");
                        check_if_rerun = false;
                    } //rerun
                    Some(false) => send_rerun
                        .send(EndState::EndProgram)
                        .expect("Channel to progress worker broken!"), //end
                }
            }
            match end_heartbeat_input.try_recv() {
                Ok(_) => {
                    println!("Got exit");
                    break;
                }
                Err(TryRecvError::Empty) => (),
                Err(_) => {
                    println!("Channel to heartbeat broken!");
                    return Err(ForkliftError::CrossbeamChannelError(
                        "Channel to heartbeat broken".to_string(),
                    ));
                }
            }
            if countdown > 5000 {
                if !responded {
                    return Err(ForkliftError::TimeoutError(format!(
                        "{} has not responded for a lifetime, please join to a different ip:port",
                        self.node_address
                    )));
                }
                countdown = 0;
                responded = false;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
            countdown += 10;
            trace!("checking socket");
            if !*has_nodelist {
                self.send_getlist()?;
            }
            self.read_and_heartbeat(&mut aio, has_nodelist)?;
            self.send_and_tickdown()?;
            if *has_nodelist {
                responded = true;
            }
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
