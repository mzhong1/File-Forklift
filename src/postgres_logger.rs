use crate::error::*;
use crate::tables::*;
use crate::SyncStats;

use crossbeam::channel::{Receiver, Sender};
use log::trace;
use postgres::Connection;
use std::sync::{Arc, Mutex};

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
pub enum EndState {
    /// End the process
    EndProgram,
    /// Rerun the program
    Rerun,
}

pub struct PostgresLogger {
    /// postgres connection
    conn: Arc<Mutex<Option<Connection>>>,
    /// channel to receive LogMessages
    input: Receiver<LogMessage>,
    /// channel to send heartbeat end signal
    end_heartbeat: Sender<EndState>,
    /// channel to send rendezvous loop end signal
    end_rendezvous: Sender<EndState>,
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
        conn: &Arc<Mutex<Option<Connection>>>,
        input: Receiver<LogMessage>,
        end_heartbeat: Sender<EndState>,
        end_rendezvous: Sender<EndState>,
    ) -> Self {
        PostgresLogger { conn: Arc::clone(conn), input, end_heartbeat, end_rendezvous }
    }
    /// Start logging messages to postgres
    pub fn start(&self) -> ForkliftResult<()> {
        let conn = self.conn.lock().unwrap();
        for log in self.input.iter() {
            match log {
                LogMessage::Error(e) => {
                    post_forklift_err(&e, &conn)?;
                }
                LogMessage::ErrorType(e, r) => {
                    post_err(e, r, &conn)?;
                }
                LogMessage::File(f) => {
                    post_update_files(&f, &conn)?;
                }
                LogMessage::Nodes(n) => {
                    post_update_nodes(&n, &conn)?;
                }
                LogMessage::TotalSync(s) => {
                    post_update_totalsync(&s, &conn)?;
                }
                LogMessage::End => {
                    if self.end_heartbeat.send(EndState::EndProgram).is_err() {
                        return Err(ForkliftError::CrossbeamChannelError(
                            "Channel to heartbeat thread broken, unable to end heartbeat"
                                .to_string(),
                        ));
                    }
                    if self.end_rendezvous.send(EndState::EndProgram).is_err() {
                        return Err(ForkliftError::CrossbeamChannelError(
                            "Channel to rendezvous thread broken, unable to end rendezvous"
                                .to_string(),
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}
