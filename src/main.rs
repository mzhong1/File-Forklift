use clap::*;
use clap::{App, Arg};
use crossbeam::channel;
use crossbeam::channel::{Receiver, Sender, TryRecvError};
use log::*;
use nng::{Protocol, Socket};
use rendezvous_hash::{DefaultNodeHasher, RendezvousNodes};
use simplelog::{CombinedLogger, Config, SharedLogger, TermLogger, WriteLogger};

use std::fs::{create_dir, File};
use std::io::{stdin, stdout, Write};
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
use crate::filesystem::FileSystemType;
use crate::input::*;
use crate::node::*;
use crate::postgres_logger::*;
use crate::rsync::*;
use crate::socket_node::*;
use crate::tables::*;

#[cfg(windows)]
const LINE_ENDING: &'static str = "\r\n";
#[cfg(not(windows))]
const LINE_ENDING: &'static str = "\n";

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
    let mut router = Socket::new(Protocol::Bus0)?;
    debug!("New router bus created");
    let current_port = full_address.port();
    router.listen(&format!("tcp://*:{}", current_port))?;
    debug!("router bound to port {}", current_port);
    router.set_nonblocking(true);
    debug!("router set to nonblocking");
    Ok(router)
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

/// Load the config file (forklift.json) from the input or default (/etc/forklift) directory
fn load_config(config_dir: &Path, name: &str) -> ForkliftResult<Input> {
    let p = config_dir.join(name);
    if !p.exists() {
        error!("{} config file does not exist", p.display());
    }
    let input = match std::fs::read_to_string(p) {
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

/// initialize the command line arguments
fn init_args() -> ForkliftResult<(String, String, Input)> {
    let matches = App::new(crate_name!())
        .author(crate_authors!())
        .about("NFS and Samba filesystem migration program")
        .version(crate_version!())
        .arg(
            Arg::with_name("configdir")
                .default_value("/etc/forklift")
                .help("The directory where the configuration file can be found")
                .long("configdir")
                .short("c")
                .takes_value(true)
                .value_name("CONFIGDIR")
                .number_of_values(1)
                .required(false),
        )
        .arg(
            Arg::with_name("username")
                .default_value("")
                .help("The username of the owner of the share")
                .long("username")
                .short("u")
                .takes_value(true)
                .value_name("USERNAME")
                .number_of_values(1)
                .required(false),
        )
        .arg(
            Arg::with_name("password")
                .default_value("")
                .help("The password of the owner of the share")
                .long("password")
                .short("p")
                .takes_value(true)
                .value_name("PASSWORD")
                .number_of_values(1)
                .required(false),
        )
        .arg(
            Arg::with_name("logfile")
                .default_value("debuglog")
                .help("Logs debug statements to file debuglog")
                .long("logfile")
                .short("l")
                .takes_value(true)
                .required(false),
        )
        .arg(Arg::with_name("v").short("v").multiple(true).help("Sets the level of verbosity"))
        .get_matches();
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
    init_logs(&path, level)?;
    debug!("Log path: {:?}", logfile);
    info!("Logs made");
    let config_dir = Path::new(matches.value_of("configdir").unwrap());
    if !config_dir.exists() {
        warn!("Config directory {} doesn't exist. Creating", config_dir.display());
        if let Err(e) = create_dir(config_dir) {
            error!("Unable to create directory {}: {}", config_dir.display(), e.to_string());
            return Err(ForkliftError::CLIError("Unable to create Config directory".to_string()));
        }
    }
    let input = load_config(config_dir, "forklift.json")?;

    let mut username = matches.value_of("username").unwrap().to_string();
    let mut password = matches.value_of("password").unwrap().to_string();
    let mut iter = 0;
    while username.is_empty() || password.is_empty() || username == LINE_ENDING.to_string() {
        if iter != 0 {
            println!("Username cannot be \\n");
        }
        match input.system {
            FileSystemType::Nfs => {
                if username.is_empty() {
                    username = "guest".to_string();
                }
                if password.is_empty() {
                    password = LINE_ENDING.to_string();
                }
            }
            FileSystemType::Samba => {
                if username.is_empty() || username == LINE_ENDING.to_string() {
                    print!("Please enter your username: ");
                    stdout().flush()?;
                    stdin().read_line(&mut username)?;
                    username = (&username).trim_end().to_string();
                }
                if password.is_empty() {
                    print!("Please enter your password: ");
                    stdout().flush()?;
                    stdin().read_line(&mut password)?;
                    password = (&password).trim_end().to_string();
                }
            }
        }
        iter += 1;
    }

    Ok((username, password, input))
}

/// Main takes in a config directory, username, password, debuglevel, and debug path. the 'v' flag
/// is used to determine debug level of the program
fn main() -> ForkliftResult<()> {
    let (username, password, input) = init_args()?;
    let (node_change_output, node_change_input) = channel::unbounded::<ChangeList>();
    let (end_heartbeat, heartbeat_input) = channel::unbounded::<EndState>();
    let (end_rendezvous, rendezvous_input) = channel::unbounded::<EndState>();
    let (log_output, log_input) = channel::unbounded::<LogMessage>();
    let config = input.clone();
    //get database url and check if we are logging anything to database
    //SOME if yes, NONE if not logging to DB
    let database_url = match input.database_url {
        Some(e) => e,
        None => String::new(),
    };

    let conn = if !database_url.is_empty() {
        //init databases;
        Some(init_connection(&database_url)?)
    } else {
        None
    };
    let postgres_logger =
        PostgresLogger::new(conn, log_input, end_heartbeat.clone(), end_rendezvous.clone());
    rayon::spawn(move || postgres_logger.start().expect("unable to log to Postgres"));
    if input.nodes.len() < 2 {
        let mess = LogMessage::ErrorType(
            ErrorType::InvalidConfigError,
            "Not enough input nodes.  Need at least 2".to_string(),
        );
        send_mess(mess, &log_output)?;
        return Err(ForkliftError::InvalidConfigError(
            "No input nodes!  Please have at least 2 node in the nodes section of your
        config file"
                .to_string(),
        ));
    }

    trace!("Attempting to get local ip address");
    let ip_address = match local_ip::get_ip(&log_output) {
        Ok(Some(ip)) => ip.ip(),
        Ok(None) => {
            send_mess(
                LogMessage::ErrorType(ErrorType::IpLocalError, "No local ip".to_string()),
                &log_output,
            )?;
            return Err(ForkliftError::IpLocalError("No local ip".to_string()));
        }
        Err(e) => {
            send_mess(
                LogMessage::ErrorType(ErrorType::IpLocalError, "No local ip".to_string()),
                &log_output,
            )?;
            return Err(e);
        }
    };
    let nodes = input.nodes.clone();
    let node_names: NodeList = NodeList::new_with_list(nodes);
    let full_address = match node_names.get_full_address(&ip_address.to_string()) {
        Some(a) => a,
        None => {
            let mess = LogMessage::ErrorType(
                ErrorType::IpLocalError,
                format!("Ip Address {} not in the node_list", ip_address),
            );
            send_mess(mess, &log_output)?;
            return Err(ForkliftError::IpLocalError(format!(
                "ip address {} not in the node list",
                ip_address
            )));
        }
    };
    debug!("current full address: {:?}", full_address);
    let current_address = SocketNode::new(full_address);
    if let Err(e) = set_current_node(&current_address) {
        send_mess(LogMessage::Error(e), &log_output)?;
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
    let auth = (&*username, &*password);
    let lifetime = input.lifetime;
    rayon::scope(|s| {
        s.spawn(|_| {
            debug!("Started Sync");
            if let Err(e) = syncer.sync(&config, auth, active_nodes.clone(), current_address) {
                // Note, only Errors if there IS a database and query/execution fails
                send_mess(LogMessage::Error(e), &log_output).expect("unable to log to postgres");
                if send_mess(LogMessage::End, &log_output).is_err() {
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
                Err(e) => send_mess(LogMessage::Error(e), &log_output),
            },
            || match rendezvous(
                &mut active_nodes.clone(),
                &node_change_input,
                &rendezvous_input,
                &log_output,
            ) {
                Ok(_) => Ok(()),
                Err(e) => send_mess(LogMessage::Error(e), &log_output),
            },
        )
    });
    Ok(())
}

/// thread where rendezvous hash is used to keep track of all active nodes
fn rendezvous(
    active_nodes: &mut Arc<Mutex<RendezvousNodes<SocketNode, DefaultNodeHasher>>>,
    node_change_input: &Receiver<ChangeList>,
    rendezvous_input: &Receiver<EndState>,
    log_output: &Sender<LogMessage>,
) -> ForkliftResult<()> {
    debug!("Started Rendezvous");
    loop {
        match rendezvous_input.try_recv() {
            Ok(_) => {
                println!("Got exit");
                let node = Nodes::new(NodeStatus::NodeFinished)?;
                send_mess(LogMessage::Nodes(node), &log_output)?;
                break;
            }
            Err(TryRecvError::Empty) => (),
            Err(_) => {
                println!("Channel to heartbeat broken!");
                break;
            }
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
