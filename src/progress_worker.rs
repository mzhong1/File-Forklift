use crossbeam::channel::{Receiver, Sender};
use std::time::Instant;

use crate::error::ForkliftResult;
use crate::filesystem_ops::SyncOutcome;
use crate::postgres_logger::{send_mess, LogMessage};
use crate::progress_message::*;
use crate::rsync::SyncStats;
use crate::tables::*;

/// threaded worker handling progress messages
pub struct ProgressWorker {
    /// source share name
    src_share: String,
    /// output printing program
    progress_info: Box<ProgressInfo + Send + Sync>,
    /// channel input for ProgressMessages
    input: Receiver<ProgressMessage>,
}

impl ProgressWorker {
    /// create a new ProgressWorker
    pub fn new(
        src_share: &str,
        progress_info: Box<ProgressInfo + Send + Sync>,
        input: Receiver<ProgressMessage>,
    ) -> ProgressWorker {
        ProgressWorker { src_share: src_share.to_string(), input, progress_info }
    }

    /// Process ProgressMessages, sending logs to postgres_logger, and track current progress through
    /// progress_info
    pub fn start(&self, send_log: &Sender<LogMessage>) -> ForkliftResult<SyncStats> {
        let mut stats = SyncStats::new();
        let mut file_done;
        let mut current_file = "".to_string();
        let mut index = 0;
        let mut total_done = 0;
        let now = Instant::now();
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
                    match x.clone() {
                        SyncOutcome::FileCopied(path, src_check, dest_check, size, update) => {
                            let file = Files::new(
                                format!("{:?}/{:?}", self.src_share, path),
                                src_check,
                                dest_check,
                                size,
                                update,
                            );
                            send_mess(LogMessage::File(file), send_log)?;
                        }
                        SyncOutcome::ChecksumUpdated(path, src_check, dest_check, size, update) => {
                            let file = Files::new(
                                format!("{:?}/{:?}", self.src_share, path),
                                src_check,
                                dest_check,
                                size,
                                update,
                            );
                            send_mess(LogMessage::File(file), send_log)?;
                        }
                        _ => {}
                    }
                    stats.add_outcome(&x);
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
        send_mess(LogMessage::End, send_log)?;
        Ok(stats)
    }
}
