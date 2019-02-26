use crate::error::*;
use crate::tables::*;
use crate::SyncStats;

use crossbeam::channel::{Receiver, Sender, TryRecvError};
use log::trace;
use r2d2::Pool;
use r2d2_postgres::PostgresConnectionManager;
use rayon::prelude::*;

#[derive(Debug)]
/// Enum holding Postgres Log Values
pub enum LogMessage {
    /// wrapper for ForkliftError
    Error(ForkliftError),
    /// wrapper for non-ForkliftError Error
    ErrorType(ErrorType, String),
    /// wrapper for File
    File(Files),
    /// wrapper for total stats
    TotalSync(SyncStats),
    /// wrapper for node change
    Nodes(Nodes),
    /// end signal
    End,
}
#[derive(Copy, Clone, Debug)]
pub enum EndState {
    /// End the process
    EndProgram,
    // Rerun the program
    //Rerun,
}

pub struct PostgresLogger {
    /// postgres connection manager
    conn_pool: Option<Pool<PostgresConnectionManager>>,
    /// channel to receive LogMessages
    input: Receiver<LogMessage>,
    /// channel to send heartbeat end signal
    end_heartbeat: Sender<EndState>,
    /// channel to send rendezvous loop end signal
    end_rendezvous: Sender<EndState>,
    /// channel to send exit to postgres loop
    send_exit: Sender<EndState>,
    /// channel to receive exit from postgres loop
    recv_exit: Receiver<EndState>,
}

/// Send a message to PostgresLogger input
pub fn send_mess(log: LogMessage, send_log: &Sender<LogMessage>) -> ForkliftResult<()> {
    trace!("Sending {:?} to postgres", log);
    if send_log.send(log).is_err() {
        return Err(ForkliftError::CrossbeamChannelError(
            "Unable to send error to postgres_logger".to_string(),
        ));
    }
    Ok(())
}

impl PostgresLogger {
    /// Create new PostgresLogger
    pub fn new(
        conn_pool: Option<Pool<PostgresConnectionManager>>,
        input: Receiver<LogMessage>,
        end_heartbeat: Sender<EndState>,
        end_rendezvous: Sender<EndState>,
    ) -> Self {
        let (send_exit, recv_exit) = crossbeam::channel::unbounded::<EndState>();
        PostgresLogger { conn_pool, input, end_heartbeat, end_rendezvous, send_exit, recv_exit }
    }
    /// Start logging messages to postgres
    pub fn start(&self) -> ForkliftResult<()> {
        loop {
            //std::thread::sleep(std::time::Duration::from_millis(10));
            let logs: Vec<_> = self.input.try_iter().collect();
            logs.into_par_iter().for_each(|log| {
                let conn = match &self.conn_pool {
                    Some(pool) => {
                        Some(pool.get().expect("Unable to get postgres connection from pool"))
                    }
                    None => None,
                };
                match log {
                    LogMessage::Error(e) => {
                        post_forklift_err(&e, &conn).expect("Add error to ErrorLogs failed");
                    }
                    LogMessage::ErrorType(e, r) => {
                        post_err(e, &r, &conn).expect("Add error to ErrorLogs failed");
                    }
                    LogMessage::File(f) => {
                        post_update_files(&f, &conn).expect("Add File to Files failed");
                    }
                    LogMessage::Nodes(n) => {
                        post_update_nodes(&n, &conn).expect("Update Node in Nodes failed");
                    }
                    LogMessage::TotalSync(s) => {
                        post_update_totalsync(&s, &conn).expect("Update TotalSync failed");
                    }
                    LogMessage::End => {
                        self.end_heartbeat
                            .send(EndState::EndProgram)
                            .expect("Channel to heartbeat thread broken, unable to end heartbeat");
                        self.end_rendezvous.send(EndState::EndProgram).expect(
                            "Channel to rendezvous thread broken, unable to end rendezvous",
                        );
                        self.send_exit
                            .send(EndState::EndProgram)
                            .expect("Channel to postgres_end broken");
                    }
                }
            });
            match self.recv_exit.try_recv() {
                Ok(_) => {
                    break;
                }
                Err(TryRecvError::Empty) => (), //keep going
                Err(_) => {
                    break;
                }
            }
        }
        Ok(())
    }
}
