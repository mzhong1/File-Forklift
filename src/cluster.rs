extern crate api;
extern crate clap;

use self::api::service_generated::*;
use error::ForkliftResult;
use message;
use nanomsg::{Error, PollFd, PollInOut, PollRequest, Socket};
use node::*;
use pulse::*;

pub struct Cluster {
    pub lifetime: u64,
    pub pulse: Pulse,
    pub names: NodeList,
    pub nodes: NodeMap,
    pub router: Socket,
}

impl Cluster {
    pub fn new(router: Socket) -> Self {
        Cluster {
            lifetime: 5,
            pulse: Pulse::new(1000),
            names: NodeList::new(),
            nodes: NodeMap::new(),
            router: router,
        }
    }

    /**
     * connect_node: &self * &str -> ForkliftResult<()>
     * REQUIRES: full_address is properly formatted as ip:port, router in self is a valid Socket
     * ENSURES: connects router to the address of full_address, output
     * error otherwise
     */
    pub fn connect_node(&mut self, full_address: &str) -> ForkliftResult<()> {
        debug!("Try to connect router to {}", full_address);
        let tcp: String = format!("tcp://{}", full_address);
        self.router.connect(&tcp)?;
        Ok(())
    }

    /**
     * add_node: &mut self * &str * bool-> null
     * REQUIRES: full_address a properly formatted ip:port, heartbeat is whether or not the added node is "live" or not,
     * router in self is properly connected
     * ENSURES: makes a new node given that the node names does not previously exist, and adds itself to both the
     * node_Names and the nodes, and connects the node to given.  Otherwise it does nothing.
     */
    pub fn add_node(&mut self, full_address: &str, heartbeat: bool) {
        if !self.names.contains_full_address(&full_address.to_string()) {
            debug!("Node names before adding {:?}", self.names.node_list);
            debug!("Node Map before adding {:?}", self.nodes.node_map);
            match self.names.add_node_to_list(&full_address) {
                Ok(t) => t,
                Err(e) => error!(
                    "Unable to parse socket address, should be in the form ip:port:{:?}",
                    e
                ),
            };
            self.nodes
                .add_node_to_map(&full_address, self.lifetime, heartbeat);
            match self.connect_node(&full_address) {
                Ok(t) => t,
                Err(e) => error!("Unable to connect to the node at ip address: {}", e),
            };
            debug!("Node names after adding {:?}", self.names.node_list);
            debug!("Node Map after adding {:?}", self.nodes.node_map);
        }
    }

    /**
     * send_getlist: &self * &str * &PollRequest -> ForkliftResult<()>
     * REQUIRES: &PollRequest a value file descriptor, pulse in self a valid Pulse, full_address a properly formatted
     * full_addr in the form of ip:port, router in self a valid socket
     * ENSURES: sends a GETLIST with the message of full_address to the cluster.
     */
    pub fn send_getlist(
        &mut self,
        full_address: &str,
        request: &PollRequest,
    ) -> ForkliftResult<()> {
        let beat = self.pulse.beat();
        if request.get_fds()[0].can_write() && beat {
            debug!("Send a GETLIST from {}", full_address);
            let message =
                message::create_message(MessageType::GETLIST, &[full_address.to_string()]);
            match self.router.nb_write(message.as_slice()) {
                Ok(..) => debug!("GETLIST sent from {}", full_address),
                Err(Error::TryAgain) => error!("Receiver not ready, message can't be sent"),
                Err(..) => error!("Failed to write to socket!"),
            };
        }
        Ok(())
    }

    /**
     * send_nodelist: &mut self * &[String] -> null
     * REQUIRES: names in self non-empty, msg_body should be non-empty, with the first and only item in the message body
     * from the GETLIST recieved message body (containing the address of the node asking for the NODELIST).  nodes in self non-empty,
     * lifetime in self > 0, router a valid connected socket
     * ENSURES: The router sends a NODELIST to the sender of a GETLIST request (although it goes to all connected nodes),
     * otherwise it does nothing if the message body of the GETLIST request is empty.  
     */
    pub fn send_nodelist(&mut self, msg_body: &[String]) {
        let address_names = self.names.to_string_vector();
        let buffer = message::create_message(MessageType::NODELIST, &address_names);

        if !msg_body.is_empty() {
            let sent_address = &msg_body[0];
            self.add_node(&sent_address, true);
            debug!("Send a NODELIST to {}", sent_address);
            match self.router.nb_write(buffer.as_slice()) {
                Ok(_) => debug!("NODELIST sent to {}!", sent_address),
                Err(Error::TryAgain) => error!("Receiver not ready, message can't be sen't"),
                Err(err) => error!("Problem while writing: {}", err),
            };
        }
    }

    /**
     * send_heartbeat: &mut self * &str -> null
     * REQUIRES: name is your full_address in the format ip:port, router in self a valid Socket
     * ENSURES: sends a HEARTBEAT message to all connected nodes
     */
    pub fn send_heartbeat(&mut self, full_address: &str) {
        debug!("Send a HEARTBEAT!");
        let buffer = vec![full_address.to_string()];
        let msg = message::create_message(MessageType::HEARTBEAT, &buffer);
        match self.router.nb_write(msg.as_slice()) {
            Ok(_) => debug!("HEARTBEAT sent!"),
            Err(Error::TryAgain) => {
                error!("Receiver not ready, message can't be sent for the moment ...")
            }
            Err(err) => error!("Problem while writing: {}", err),
        };
    }

    /**
     * tickdown_nodes: &mut self -> null
     * REQUIRES: nodes in self not empty, names in self not empty
     * ENSURES: for all nodes that have not sent a HEARTBEAT message to you within
     * a second, tickdown their liveness.  For all nodes that HAVE sent you a
     * HEARTBEAT message, reset their has_heartbeat value to false
     */
    pub fn tickdown_nodes(&mut self) {
        trace!("Tickdown and reset nodes");
        for name in &self.names.to_string_vector() {
            self.nodes.node_map.entry(name.to_string()).and_modify(|n| {
                if !n.has_heartbeat {
                    n.tickdown();
                } else {
                    n.has_heartbeat = false;
                    debug!("HEARTBEAT was heard for node {:?}", n);
                }
            });
        }
    }

    /**
     * send_and_tickdown: &PollRequest * &mut u64 * &str * &mut Socket * u64 * &mut HashMap<String, Node> * &mut Vec<SocketAddr> -> ForkliftRequest<()>
     * REQUIRES: request is a valid vector of PollRequests, pulse a valid Pulse object
     * name is your full address in the form ip:port, router a valid Socket,
     * nodes not empty, node_names not empty
     * ENSURES: returns Ok(()) if successfully sending a heartbeat to connected nodes and ticking down,
     * otherwise return Err
     */
    pub fn send_and_tickdown(&mut self, full_address: &str, request: &PollRequest) {
        if request.get_fds()[0].can_write() {
            let beat = self.pulse.beat();
            if beat {
                self.send_heartbeat(full_address);
                self.tickdown_nodes();
            }
        }
    }

    /**
     * read_message_to_u8: &self-> Vec<u8>
     * REQUIRES: router in self a valid working socket
     * ENSURES: returns the next message queued to the router as a Vec<u8>
     */
    pub fn read_message_to_u8(&mut self) -> Vec<u8> {
        let mut buffer = Vec::new();
        match self.router.nb_read_to_end(&mut buffer) {
            Ok(_) => debug!("Read message {} bytes!", buffer.len()),
            Err(Error::TryAgain) => error!("Nothing to be read"),
            Err(err) => error!("Problem while reading: {}", err),
        };
        buffer
    }

    /**
     * parse_nodelist_message: &self * &mut bool * &[u8] -> null
     * REQUIRES: buf a message read from the socket, node_names in self not empty, nodes in self not empty, lifetime in self > 0,
     * router in self a valid Socket, has_nodelist is false
     * ENSURES: parses a NODELIST message into a node_list and creates/adds the nodes received to the cluster
     */
    pub fn parse_nodelist_message(&mut self, has_nodelist: &mut bool, buf: &[u8]) {
        if !*has_nodelist {
            debug!("Parse the NODELIST!");
            let list = match message::read_message(buf) {
                Some(t) => t,
                None => {
                    error!("NODELIST message is empty");
                    vec![]
                }
            };
            for l in &list {
                self.add_node(&l, false)
            }
            if !list.is_empty() {
                *has_nodelist = true;
            }
        }
    }

    /**
     * heartbeat_heard: &[String] * &mut Vec<SocketAddr> * &mut HashMap<String, Node> * i64 * &mut Socket &str -> null
     * REQUIRES: msg_body not empty, node_names not empty, nodes not empty, lifetime the lifetime of a node, router a
     * valid Socket, full_address a properly formatted ip:port string
     * ENSURES: updates the hashmap to either: add a new node if the heartbeart came from a new node,
     * or updates the liveness of the node the heartbeat came from
     */
    pub fn heartbeat_heard(&mut self, msg_body: &[String]) {
        if !msg_body.is_empty() {
            let sent_address = &msg_body[0];
            self.add_node(&sent_address, true);
            self.nodes
                .node_map
                .entry(sent_address.to_string())
                .and_modify(|n| {
                    let reactive = n.heartbeat();
                });
        }
    }

    /**
     * read_and_heartbeat: &mut self * &PollRequest * &mut bool * &str -> null
     * REQUIRES: request not empty, router in self is connected, names in self not empty, nodes in self not empty,
     * lifetime in self > 0, pulse in self a valid Pulse object, full_address is properly formatted as ip:port,
     * ENSURES: reads incoming messages and sends out heartbeats every interval milliseconds.  
     */
    pub fn read_and_heartbeat(
        &mut self,
        request: &PollRequest,
        has_nodelist: &mut bool,
        full_address: &str,
    ) {
        if request.get_fds()[0].can_read() {
            //check message type
            let msg = self.read_message_to_u8();
            let msgtype = message::get_message_type(&msg);
            let msg_body = match message::read_message(&msg) {
                Some(t) => t,
                None => {
                    error!("Message body is empty. Ignore the message");
                    vec![]
                }
            };
            match msgtype {
                MessageType::NODELIST => {
                    debug!("Can read message of type NODELIST");
                    self.parse_nodelist_message(has_nodelist, &msg)
                }
                MessageType::GETLIST => {
                    debug!("Can read message of type GETLIST");
                    self.send_nodelist(&msg_body)
                }
                MessageType::HEARTBEAT => {
                    debug!("Can read message of type HEARTBEAT");
                    self.heartbeat_heard(&msg_body);
                    if !*has_nodelist {
                        match self.send_getlist(full_address, request) {
                            Ok(t) => t,
                            Err(e) => {
                                error!("Time ran backwards!  Abort! {}", e);
                                panic!("Time ran backwards! Abort! {}", e)
                            }
                        };
                    }
                }
            }
        }
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
        full_address: &str,
        has_nodelist: &mut bool,
    ) -> ForkliftResult<()> {
        let mut countdown = 0;
        loop {
            if countdown > 5000 && !*has_nodelist {
                panic!(
                    "{} has not responded for a lifetime, please join to a different ip:port",
                    full_address
                );
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
                        panic!("Time ran backwards! Abort! {}", e)
                    }
                };
            }

            self.read_and_heartbeat(&request, has_nodelist, full_address);

            self.send_and_tickdown(full_address, &request);
        }
        //Ok(())
    }

    pub fn init_connect(&mut self, full_address: &str) {
        trace!("Initializing connection...");
        for node_ip in self.names.node_list.clone() {
            if node_ip.to_string() != full_address {
                trace!("Attempting to connect to {}", node_ip);
                match self.connect_node(&node_ip.to_string()) {
                    Ok(t) => t,
                    Err(e) => error!(
                        "Error: {} Unable to connect to the node at ip address: {}",
                        e, full_address
                    ),
                };
            }
        }
    }
}
