use crossbeam::channel::{Receiver, Sender};
use std::time::Instant;

use crate::error::ForkliftResult;
use crate::filesystem_ops::SyncOutcome;
use crate::postgres_logger::{send_mess, LogMessage};
use crate::progress_message::*;
use crate::rsync::SyncStats;
use crate::tables::*;

pub struct ProgressWorker {
    input: Receiver<ProgressMessage>,
    progress_info: Box<ProgressInfo + Send>,
    src_share: String,
}

impl ProgressWorker {
    pub fn new(
        input: Receiver<ProgressMessage>,
        progress_info: Box<ProgressInfo + Send>,
        src_share: String,
    ) -> ProgressWorker {
        ProgressWorker {
            input,
            progress_info,
            src_share,
        }
    }

    //NOte: Figure out a way to send off postgres in this function
    pub fn start(&self, send_log: &Sender<LogMessage>) -> ForkliftResult<SyncStats> {
        let mut stats = SyncStats::new();
        let mut file_done = 0;
        let mut current_file = "".to_string();
        let mut index = 0;
        let mut total_done = 0;
        let now = Instant::now();
        for progress in self.input.iter() {
            match progress {
                ProgressMessage::Todo {
                    num_files,
                    total_size,
                } => {
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
                    file_done = 0;
                }
                ProgressMessage::SendError(error) => {
                    send_mess(LogMessage::Error(error), send_log)?;
                }
                ProgressMessage::CheckSyncing { done, size, .. } => {
                    file_done = done;
                    total_done = done;
                    let elapsed = now.elapsed().as_secs() as usize;
                    let eta =
                        if total_done == 0 || ((elapsed * stats.tot_size) / total_done) < elapsed {
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
            }
            send_mess(LogMessage::TotalSync(stats.clone()), send_log)?;
        }
        self.progress_info.end(&stats);
        Ok(stats)
    }
}
