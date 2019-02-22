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
use crate::progress_message::ProgressMessage;
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

/// Given a socket address, create a new socket with the Bus Protocol bound to the
/// input address
fn init_router(full_address: &SocketAddr) -> ForkliftResult<Socket> {
    debug!("Initializing router");
    let mut router = Socket::new(Protocol::Bus)?;
    debug!("New router bus created");
    let current_port = full_address.port();
    router.bind(&format!("tcp://*:{}", current_port))?;
    debug!("router bound to port {}", current_port);
    Ok(router)
}

/// parse the command line entry for the config file.  
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
    Ok(Input::new_input(&input)?)
}

/// run the heartbeat protocol
fn heartbeat(
    lifetime: u64,
    node_names: NodeList,
    joined: &mut bool,
    node_address: SocketAddr,
    heartbeat_input: &Receiver<EndState>,
    node_change_output: Sender<ChangeList>,
    log_output: Sender<LogMessage>,
) -> ForkliftResult<()> {
    let mess = ChangeList::new(ChangeType::AddNode, SocketNode::new(node_address));
    if node_change_output.send(mess).is_err() {
        return Err(ForkliftError::CrossbeamChannelError(
            "Channel to rendezvous is broken!".to_string(),
        ));
    }
    let router = match init_router(&node_address) {
        Ok(t) => t,
        Err(e) => {
            error!("Error {:?}, Unable to connect router!", e);
            return Err(e);
        }
    }; //Make the node
    std::thread::sleep(std::time::Duration::from_millis(10));
    let mut cluster = Cluster::new(lifetime, router, &node_address, node_change_output, log_output);
    cluster.nodes = NodeMap::init_nodemap(&node_address, cluster.lifetime, &node_names.node_list)?; //create mutable hashmap of nodes
                                                                                                    //sleep for a bit to let other nodes start up
    cluster.names = node_names;
    cluster.init_connect()?;
    cluster.heartbeat_loop(joined, &heartbeat_input)
}

/// initialize Loggers to console and file
fn init_logs(path: &Path, level: simplelog::LevelFilter) -> ForkliftResult<()> {
    if !path.exists() {
        File::create(path)?;
    }
    let mut loggers: Vec<Box<dyn SharedLogger>> = vec![];
    if let Some(term_logger) = TermLogger::new(level, Config::default()) {
        loggers.push(term_logger);
    }
    loggers.push(WriteLogger::new(level, Config::default(), File::create(path)?));
    let _ = CombinedLogger::init(loggers);

    Ok(())
}

/// Main takes in a config file, username, password, debuglevel, and debug path. the 'v' flag
/// is used to determine debug level of the program
fn main() -> ForkliftResult<()> {
    let matches = App::new(crate_name!())
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
            return Err(ForkliftError::CLIError("Home directory not found".to_string()));
        }
    };
    let mut username = matches.value_of("username").unwrap();
    if username.is_empty() {
        username = "guest";
    }
    let mut password = matches.value_of("password").unwrap();
    if password.is_empty() {
        password = "\n";
    }
    init_logs(&path, level)?;
    debug!("Log path: {:?}", logfile);
    info!("Logs made");

    let (node_change_output, node_change_input) = channel::unbounded::<ChangeList>();
    let (end_heartbeat, heartbeat_input) = channel::unbounded::<EndState>();
    let (end_rendezvous, rendezvous_input) = channel::unbounded::<EndState>();
    let (log_output, log_input) = channel::unbounded::<LogMessage>();

    let input = parse_matches(&matches)?;
    //get database url and check if we are logging anything to database
    //SOME if yes, NONE if not logging to DB
    let database_url = match input.database_url {
        Some(e) => e,
        None => String::new(),
    };

    let conn = if !database_url.is_empty() {
        //init databases;
        Some(init_connection(database_url.clone())?)
    } else {
        None
    };
    let postgres_logger = PostgresLogger::new(
        &Arc::new(Mutex::new(conn)),
        log_input,
        end_heartbeat.clone(),
        end_rendezvous.clone(),
    );
    rayon::spawn(move || postgres_logger.start().unwrap());
    if input.nodes.len() < 2 {
        let mess = LogMessage::ErrorType(
            ErrorType::InvalidConfigError,
            "Not enough input nodes.  Need at least 2".to_string(),
        );
        send_mess(mess, &log_output.clone())?;
        return Err(ForkliftError::InvalidConfigError(
            "No input nodes!  Please have at least 2 node in the nodes section of your
        config file"
                .to_string(),
        ));
    }

    trace!("Attempting to get local ip address");
    let ip_address = match local_ip::get_ip(&log_output.clone()) {
        Ok(Some(ip)) => ip.ip(),
        Ok(None) => {
            send_mess(
                LogMessage::ErrorType(ErrorType::IpLocalError, "No local ip".to_string()),
                &log_output.clone(),
            )?;
            return Err(ForkliftError::IpLocalError("No local ip".to_string()));
        }
        Err(e) => {
            send_mess(
                LogMessage::ErrorType(ErrorType::IpLocalError, "No local ip".to_string()),
                &log_output.clone(),
            )?;
            return Err(e);
        }
    };
    let nodes = input.nodes.clone();
    let node_names: NodeList = NodeList::new_with_list(nodes.clone());
    let full_address = match node_names.get_full_address(&ip_address.to_string()) {
        Some(a) => a,
        None => {
            let mess = LogMessage::ErrorType(
                ErrorType::IpLocalError,
                format!("Ip Address {} not in the node_list", ip_address),
            );
            send_mess(mess, &log_output.clone())?;
            return Err(ForkliftError::IpLocalError(format!(
                "ip address {} not in the node list",
                ip_address
            )));
        }
    };
    debug!("current full address: {:?}", full_address);
    let current_address = SocketNode::new(full_address);
    if let Err(e) = set_current_node(&current_address) {
        send_mess(LogMessage::Error(e), &log_output.clone())?;
    };
    let mut joined = input.nodes.len() != 2;
    let console_info = ConsoleProgressOutput::new();
    let system = input.system;

    let send_nodes = RendezvousNodes::default();
    let active_nodes = Arc::new(Mutex::new(send_nodes));

    let syncer = Rsyncer::new(
        input.src_path,
        input.dest_path,
        system,
        Box::new(console_info),
        log_output.clone(),
    );
    let servers = (&*input.src_server, &*input.dest_server);
    let shares = (&*input.src_share, &*input.dest_share);
    let levels = (input.debug_level, input.num_threads);
    let auth = (input.workgroup, username.to_string(), password.to_string());
    let lifetime = input.lifetime;
    rayon::scope(|s| {
        s.spawn(|_| {
            debug!("Started Sync");
            if let Err(e) =
                syncer.sync(servers, shares, levels, auth, active_nodes.clone(), current_address)
            {
                // Note, only Errors if there IS a database and query/execution fails
                send_mess(LogMessage::Error(e), &log_output.clone()).unwrap();
                if send_mess(LogMessage::End, &log_output.clone()).is_err() {
                    error!(
                        "Channel to postgres_logger is broken, attempting to manually end program"
                    );
                    if end_heartbeat.clone().send(EndState::EndProgram).is_err() {
                        panic!("Unable to end heartbeat");
                    }
                    if end_rendezvous.clone().send(EndState::EndProgram).is_err() {
                        panic!("Unable to end rendezvous");
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
                &heartbeat_input,
                node_change_output,
                log_output.clone(),
            ) {
                Ok(_) => Ok(()),
                Err(e) => send_mess(LogMessage::Error(e), &log_output.clone()),
            },
            || match rendezvous(
                &mut active_nodes.clone(),
                &node_change_input,
                &rendezvous_input,
                &log_output.clone(),
            ) {
                Ok(_) => Ok(()),
                Err(e) => send_mess(LogMessage::Error(e), &log_output.clone()),
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
    node_change_input: &Receiver<ChangeList>,
    heartbeat_input: &Receiver<EndState>,
    log_output: &Sender<LogMessage>,
) -> ForkliftResult<()> {
    debug!("Started Rendezvous");
    loop {
        if heartbeat_input.try_recv().is_ok() {
            println!("Got exit");
            let node = Nodes::new(NodeStatus::NodeFinished)?;
            send_mess(LogMessage::Nodes(node), &log_output)?;
            break;
        }
        let mut list = match active_nodes.lock() {
            Ok(arr) => arr,
            Err(e) => {
                let mess = LogMessage::ErrorType(
                    ErrorType::PoisonedMutexError,
                    format!("Error {:?}, Poisoned rendezvous mutex", e),
                );
                send_mess(mess, &log_output)?;
                return Err(ForkliftError::FSError("Poisoned Rendezvous.".to_string()));
            }
        };
        if let Ok(change) = node_change_input.try_recv() {
            match change.change_type {
                ChangeType::AddNode => {
                    info!("Add Node {:?} to active list!", change.socket_node);
                    list.insert(change.socket_node);
                    let node = Nodes::new(NodeStatus::NodeAdded)?;
                    send_mess(LogMessage::Nodes(node), &log_output)?;
                    info!("The current list is {:?}", list.calc_candidates(&1).collect::<Vec<_>>());
                }
                ChangeType::RemNode => {
                    info!("Remove Node {:?} from active list!", change.socket_node);
                    list.remove(&change.socket_node);
                    let node = Nodes::new(NodeStatus::NodeDied)?;
                    send_mess(LogMessage::Nodes(node), &log_output)?;
                    info!("The current list is {:?}", list.calc_candidates(&1).collect::<Vec<_>>());
                }
            };
        }
    }
    Ok(())
}
