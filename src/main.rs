use clap::*;
use clap::{App, Arg};
use crossbeam::channel;
use crossbeam::channel::{Receiver, Sender};
use log::*;
use nanomsg::{Protocol, Socket};
use rendezvous_hash::{DefaultNodeHasher, RendezvousNodes};
use simplelog::{CombinedLogger, Config, SharedLogger, TermLogger, WriteLogger};

use std::fs::File;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};

mod cluster;
mod console_output;
mod error;
mod filesystem;
mod filesystem_entry;
mod filesystem_ops;
mod input;
mod local_ip;
mod message;
mod node;
mod postgres_logger;
mod progress_message;
mod progress_worker;
mod pulse;
mod rsync;
mod rsync_worker;
mod socket_node;
mod tables;
mod walk_worker;

use crate::cluster::Cluster;
use crate::console_output::ConsoleProgressOutput;
use crate::error::{ForkliftError, ForkliftResult};
use crate::input::*;
use crate::node::*;
use crate::postgres_logger::*;
use crate::rsync::*;
use crate::socket_node::*;
use crate::tables::*;

#[test]
fn test_init_router() {
    match init_router(&SocketAddr::new(
        ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(10, 26, 24, 92)),
        5555,
    )) {
        Ok(s) => s,
        Err(e) => {
            error!("Error {}", e);
            panic!("Router cannot bind to port")
        }
    };
}

/**
 * init_router: &str -> ForkliftResult<Socket>
 * REQUIRES: full_address a string in the form ip:port, where
 * ip is your local ip and port is the port your node will bind to
 * ENSURES: returns a Result<Socket,Err> where if successful, returns
 * a new socket with the Bus Protocol bound to the input port.  Otherwise,
 * return the associated ForkliftError
 */
fn init_router(full_address: &SocketAddr) -> ForkliftResult<Socket> {
    debug!("Initializing router");
    let mut router = Socket::new(Protocol::Bus)?;
    debug!("New router bus created");
    let current_port = full_address.port();
    router.bind(&format!("tcp://*:{}", current_port))?;
    debug!("router bound to port {}", current_port);
    Ok(router)
}

fn parse_matches(matches: &clap::ArgMatches<'_>) -> ForkliftResult<Input> {
    let path = match matches.value_of("config") {
        None => Path::new(""),
        Some(t) => Path::new(t),
    };
    let input = match std::fs::read_to_string(path) {
        Ok(e) => e,
        Err(e) => {
            error!("{:?}, Unable to read file", e);
            return Err(ForkliftError::InvalidConfigError(format!(
                "Error {:?}, unable to read file",
                e
            )));
        }
    };
    Ok(Input::new(&input))
}

fn heartbeat(
    lifetime: u64,
    node_names: NodeList,
    joined: &mut bool,
    full_address: SocketAddr,
    s: Sender<ChangeList>,
    recv_end: &Receiver<EndState>,
    send_log: Sender<LogMessage>,
) -> ForkliftResult<()> {
    let mess = ChangeList::new(ChangeType::AddNode, SocketNode::new(full_address));
    if s.send(mess).is_err() {
        return Err(ForkliftError::CrossbeamChannelError(
            "Channel to rendezvous is broken!".to_string(),
        ));
    }
    let router = match init_router(&full_address) {
        Ok(t) => t,
        Err(e) => {
            error!("Error {:?}, Unable to connect router!", e);
            return Err(e);
        }
    }; //Make the node
    std::thread::sleep(std::time::Duration::from_millis(10));
    let mut cluster = Cluster::new(lifetime, router, &full_address, s, send_log);
    cluster.nodes = NodeMap::init_nodemap(&full_address, cluster.lifetime, &node_names.node_list)?; //create mutable hashmap of nodes
                                                                                                    //sleep for a bit to let other nodes start up
    cluster.names = node_names;
    cluster.init_connect(&full_address)?;
    cluster.heartbeat_loop(&full_address, joined, &recv_end)
}

fn init_logs(f: &Path, level: simplelog::LevelFilter) -> ForkliftResult<()> {
    if !f.exists() {
        File::create(f)?;
    }
    let mut loggers: Vec<Box<dyn SharedLogger>> = vec![];
    if let Some(term_logger) = TermLogger::new(level, Config::default()) {
        loggers.push(term_logger);
    }
    loggers.push(WriteLogger::new(level, Config::default(), File::create(f)?));
    let _ = CombinedLogger::init(loggers);

    Ok(())
}

/*
    main takes in two flags:
    j: computer is a new node, not a part of the original list
    d: create debug logs
    When the 'j' flag is raised, the program takes in the arguments ip_addr:port, otherip_addr:port
    Without the 'j' flag, the program takes in a file argument of ip_addr:port
    addresses of all nodes in the graph
*/
fn main() -> ForkliftResult<()> {
    let matches = App::new("Heartbeat Logs")
        .author(crate_authors!())
        .about("NFS and Samba filesystem migration program")
        .version(crate_version!())
        .arg(
            Arg::with_name("config")
                .help("The name of the JSON file storing the cluster configuration for the node")
                .long_help("The name of the JSON file storing the cluster configurations for the node, formatted in JSON as nodes: [SocketAddresses], src_server: 'name of source server', dest_server: 'name of destination server', src_share: 'name of source share', dest_share: 'name of destination share'")
                .long("config")
                .short("c")
                .takes_value(true)
                .value_name("CONFIGFILE")
                .number_of_values(1)
                .required(true)
        ).arg(
            Arg::with_name("username")
                .help("The username of the owner of the share")
                .long("username")
                .short("u")
                .takes_value(true)
                .value_name("USERNAME")
                .number_of_values(1)
                .required(true)
        ).arg(
            Arg::with_name("password")
                .help("The password of the owner of the share")
                .long("password")
                .short("p")
                .takes_value(true)
                .value_name("PASSWORD")
                .number_of_values(1)
                .required(true)
        ).arg(
            Arg::with_name("logfile")
                .default_value("debuglog")
                .help("Logs debug statements to file debuglog")
                .long("logfile")
                .short("l")
                .takes_value(true)
                .required(false),
        ).arg(
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the level of verbosity"),
        ).get_matches();
    let level = match matches.occurrences_of("v") {
        0 => simplelog::LevelFilter::Info,
        1 => simplelog::LevelFilter::Debug,
        _ => simplelog::LevelFilter::Trace,
    };
    let logfile = matches.value_of("logfile").unwrap();
    let path = match dirs::home_dir() {
        Some(path) => path.join(logfile),
        None => {
            return Err(ForkliftError::CLIError(
                "Home directory not found".to_string(),
            ));
        }
    };
    let username = match matches.value_of("username") {
        Some(e) => {
            if e.is_empty() {
                "guest"
            } else {
                e
            }
        }
        None => {
            return Err(ForkliftError::CLIError("username not found".to_string()));
        }
    };
    let password = match matches.value_of("password") {
        Some(e) => {
            if e.is_empty() {
                "\n"
            } else {
                e
            }
        }
        None => {
            return Err(ForkliftError::CLIError("password not found".to_string()));
        }
    };
    init_logs(&path, level)?;
    debug!("Log path: {:?}", logfile);
    info!("Logs made");

    let (send, recv) = channel::unbounded::<ChangeList>();
    let (send_end, recv_end) = channel::unbounded::<EndState>();
    let (send_rend, recv_rend) = channel::unbounded::<EndState>();

    let input = parse_matches(&matches)?;
    //get database url and check if we are logging anything to database
    //SOME if yes, NONE if not logging to DB
    let database = match input.database_url {
        Some(e) => e,
        None => String::new(),
    };

    let (send_log, recv_log) = channel::unbounded::<LogMessage>();

    let conn = if !database.is_empty() {
        //init databases;
        Some(init_connection(database.clone())?)
    } else {
        None
    };
    let p_logger = PostgresLogger::new(
        &Arc::new(Mutex::new(conn)),
        recv_log,
        send_end.clone(),
        send_rend.clone(),
    );
    rayon::spawn(move || p_logger.start().unwrap());
    if input.nodes.len() < 2 {
        let mess = LogMessage::ErrorType(
            ErrorType::InvalidConfigError,
            "Not enough input nodes.  Need at least 2".to_string(),
        );
        send_mess(mess, &send_log.clone())?;
        return Err(ForkliftError::InvalidConfigError(
            "No input nodes!  Please have at least 2 node in the nodes section of your
        config file"
                .to_string(),
        ));
    }

    trace!("Attempting to get local ip address");
    let ip_address = match local_ip::get_ip(send_log.clone()) {
        Ok(Some(ip)) => ip.ip(),
        Ok(None) => {
            send_mess(
                LogMessage::ErrorType(ErrorType::IpLocalError, "No local ip".to_string()),
                &send_log.clone(),
            )?;
            return Err(ForkliftError::IpLocalError("No local ip".to_string()));
        }
        Err(e) => {
            send_mess(
                LogMessage::ErrorType(ErrorType::IpLocalError, "No local ip".to_string()),
                &send_log.clone(),
            )?;
            return Err(e);
        }
    };
    let nodes = input.nodes.clone();
    let node_names: NodeList = NodeList::new_with_list(nodes.clone());
    let full_address = match node_names.get_full_address(&ip_address.to_string()) {
        Some(a) => a,
        None => {
            send_mess(
                LogMessage::ErrorType(
                    ErrorType::IpLocalError,
                    format!("Ip Address {} not in the node_list", ip_address),
                ),
                &send_log.clone(),
            )?;
            return Err(ForkliftError::IpLocalError(format!(
                "ip address {} not in the node list",
                ip_address
            )));
        }
    };
    debug!("current full address: {:?}", full_address);
    let mine = SocketNode::new(full_address);
    if let Err(e) = set_current_node(&mine) {
        send_mess(LogMessage::Error(e), &send_log.clone())?;
    };
    let mut joined = input.nodes.len() != 2;
    let console_info = ConsoleProgressOutput::new();
    let system = input.system;

    let send_nodes = RendezvousNodes::default();
    let active_nodes = Arc::new(Mutex::new(send_nodes));

    let syncer = Rsyncer::new(
        system,
        Box::new(console_info),
        send_log.clone(),
        input.src_path,
        input.dest_path,
    );

    let (src_server, dest_server) = (input.src_server, input.dest_server);
    let (src_share, dest_share) = (input.src_share, input.dest_share);
    let (debug_level, num_threads) = (input.debug_level, input.num_threads);
    let workgroup = input.workgroup;
    let lifetime = input.lifetime;
    rayon::scope(|s| {
        s.spawn(|_| {
            println!("Started Sync");
            match syncer.sync(
                (&src_server, &dest_server),
                (&src_share, &dest_share),
                (debug_level, num_threads),
                (workgroup, username.to_string(), password.to_string()),
                active_nodes.clone(),
                mine,
            ) {
                Ok(_) => (),
                Err(e) => {
                    // Note, only Errors if there IS a database and query/execution fails
                    send_mess(LogMessage::Error(e), &send_log.clone()).unwrap();
                    if send_mess(LogMessage::End, &send_log.clone()).is_err() {
                        error!("Channel to postgres_logger is broken, attempting to manually end program");
                        if send_end.clone().send(EndState::EndProgram).is_err()
                        {
                            panic!("Unable to end heartbeat");
                        }
                        if send_rend.clone().send(EndState::EndProgram).is_err()
                        {
                            panic!("Unable to end rendezvous");
                        }
                    }
                }
            }
        });

        rayon::join(
            || match heartbeat(
                lifetime,
                node_names,
                &mut joined,
                full_address,
                send,
                &recv_end,
                send_log.clone(),
            ) {
                Ok(_) => Ok(()),
                Err(e) => send_mess(LogMessage::Error(e), &send_log.clone()),
            },
            || match rendezvous(
                &mut active_nodes.clone(),
                &recv,
                &recv_rend,
                &send_log.clone(),
            ) {
                Ok(_) => Ok(()),
                Err(e) => send_mess(LogMessage::Error(e), &send_log.clone()),
            },
        )
    });
    Ok(())
}

/**
 * Thread where rendezvous hash is dealt with
 */
fn rendezvous(
    active_nodes: &mut Arc<Mutex<RendezvousNodes<SocketNode, DefaultNodeHasher>>>,
    r: &Receiver<ChangeList>,
    recv_end: &Receiver<EndState>,
    send_log: &Sender<LogMessage>,
) -> ForkliftResult<()> {
    debug!("Started Rendezvous");
    loop {
        if recv_end.try_recv().is_ok() {
            println!("Got exit");
            let node = Nodes::new(NodeStatus::NodeFinished)?;
            send_mess(LogMessage::Nodes(node), &send_log)?;
            break;
        }
        let mut list = match active_nodes.lock() {
            Ok(l) => l,
            Err(e) => {
                send_mess(
                    LogMessage::ErrorType(
                        ErrorType::PoisonedMutexError,
                        format!("Error {:?}, Poisoned rendezvous mutex", e),
                    ),
                    &send_log,
                )?;
                return Err(ForkliftError::FSError("Poisoned Rendezvous.".to_string()));
            }
        };
        match r.try_recv() {
            Ok(c) => {
                match c.change_type {
                    ChangeType::AddNode => {
                        info!("Add Node {:?} to active list!", c.socket_node);
                        list.insert(c.socket_node);
                        let node = Nodes::new(NodeStatus::NodeAdded)?;
                        send_mess(LogMessage::Nodes(node), &send_log)?;
                        info!(
                            "The current list is {:?}",
                            list.calc_candidates(&1).collect::<Vec<_>>()
                        );
                    }
                    ChangeType::RemNode => {
                        info!("Remove Node {:?} from active list!", c.socket_node);
                        list.remove(&c.socket_node);
                        let node = Nodes::new(NodeStatus::NodeDied)?;
                        send_mess(LogMessage::Nodes(node), &send_log)?;
                        info!(
                            "The current list is {:?}",
                            list.calc_candidates(&1).collect::<Vec<_>>()
                        );
                    }
                };
            }
            Err(_) => trace!("No Changes"),
        }
    }
    Ok(())
}
