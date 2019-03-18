use crossbeam::channel::{Receiver, Sender};
use std::time::Instant;

use crate::error::{ForkliftError, ForkliftResult};
use crate::filesystem_ops::SyncOutcome;
use crate::postgres_logger::{send_mess, EndState, LogMessage};
use crate::progress_message::*;
use crate::rsync::SyncStats;
use crate::tables::*;

/// threaded worker handling progress messages
pub struct ProgressWorker {
    /// source share name
    src_share: String,
    /// destination share name
    dest_share: String,
    /// output printing program
    progress_info: Box<ProgressInfo + Send + Sync>,
    /// channel input for ProgressMessages
    input: Receiver<ProgressMessage>,
    /// channel output for is rerun
    is_rerun: Sender<EndState>,
    /// channel input for is rerun
    end_run: Receiver<EndState>,
}

impl ProgressWorker {
    /// create a new ProgressWorker
    pub fn new(
        src_share: &str,
        dest_share: &str,
        progress_info: Box<ProgressInfo + Send + Sync>,
        input: Receiver<ProgressMessage>,
        is_rerun: Sender<EndState>,
        end_run: Receiver<EndState>,
    ) -> ProgressWorker {
        ProgressWorker {
            src_share: src_share.to_string(),
            dest_share: dest_share.to_string(),
            input,
            progress_info,
            is_rerun,
            end_run,
        }
    }

    /// Process ProgressMessages, sending logs to postgres_logger, and track current progress through
    /// progress_info
    pub fn start(
        &self,
        send_log: &Sender<LogMessage>,
        get_signal: &Sender<EndState>,
    ) -> ForkliftResult<SyncStats> {
        let mut stats = SyncStats::new();
        let mut file_done;
        let mut current_file = "".to_string();
        let mut index = 0;
        let mut total_done = 0;
        let now = Instant::now();
        loop {
            self.progress_info.start(&self.src_share, &self.dest_share);
            for progress in self.input.iter() {
                match progress {
                    ProgressMessage::Todo { num_files, total_size } => {
                        stats.tot_files += num_files;
                        stats.tot_size += total_size;
                    }
                    ProgressMessage::StartSync(x) => {
                        self.progress_info.new_file(&x);
                        current_file = x;
                        index += 1;
                    }
                    ProgressMessage::DoneSyncing(x) => {
                        self.progress_info.done_syncing();
                        stats.add_outcome(&x);
                        match x {
                            SyncOutcome::FileCopied(path, src_check, dest_check, size, update) => {
                                let file = Files::new(
                                    &format!("{:?}{:?}", self.src_share, path),
                                    src_check,
                                    dest_check,
                                    size,
                                    update,
                                );
                                send_mess(LogMessage::File(file), send_log)?;
                            }
                            SyncOutcome::ChecksumUpdated(
                                path,
                                src_check,
                                dest_check,
                                size,
                                update,
                            ) => {
                                let file = Files::new(
                                    &format!("{:?}{:?}", self.src_share, path),
                                    src_check,
                                    dest_check,
                                    size,
                                    update,
                                );
                                send_mess(LogMessage::File(file), send_log)?;
                            }
                            _ => {}
                        }
                    }
                    ProgressMessage::SendError(error) => {
                        send_mess(LogMessage::Error(error), send_log)?;
                    }
                    ProgressMessage::CheckSyncing { done, size, .. } => {
                        file_done = done;
                        total_done += done;
                        let elapsed = now.elapsed().as_secs() as usize;
                        let eta = if total_done == 0 {
                            elapsed
                        } else {
                            ((elapsed * stats.tot_size) / total_done) - elapsed
                        };
                        let detailed_progress = Progress {
                            current_file: current_file.clone(),
                            file_done,
                            file_size: size,
                            total_done,
                            total_size: stats.tot_size,
                            index,
                            num_files: stats.tot_files as usize,
                            eta,
                        };
                        self.progress_info.progress(&detailed_progress);
                    }
                    ProgressMessage::EndSync => {
                        break;
                    }
                }
                send_mess(LogMessage::TotalSync(stats), send_log)?;
            }
            self.progress_info.end(&stats);
            if self.is_rerun.send(EndState::EndProgram).is_err() {
                let mess = LogMessage::Error(ForkliftError::CrossbeamChannelError("Channel to Heartbeat for is rerun broken! Cannot determine if program should rerun".to_string()));
                send_mess(mess, send_log)?;
            }

            match self.end_run.recv() {
                Ok(EndState::EndProgram) => break,
                Ok(EndState::Rerun) => {
                    if let Err(e) = get_signal.send(EndState::Rerun) {
                        let mess = LogMessage::Error(ForkliftError::CrossbeamChannelError("Channel to RSync for rerun is broken! Cannot determine if program should rerun".to_string()));
                        send_mess(mess, send_log)?;
                    }
                }
                Err(e) => {
                    let err = ForkliftError::CrossbeamChannelError("Channel to Heartbeat for is rerun broken! Cannot determine if program should rerun".to_string());
                    let mess = LogMessage::Error(err);
                    send_mess(mess, send_log)?;
                    break;
                }
            }
        }
        send_mess(LogMessage::End, send_log)?;
        if let Err(e) = get_signal.send(EndState::EndProgram) {
            let mess = LogMessage::Error(ForkliftError::CrossbeamChannelError(
                "Channel to RSync for End program is broken! Cannot determine if program should rerun"
                    .to_string(),
            ));
            send_mess(mess, send_log)?;
        }
        Ok(stats)
    }
}
