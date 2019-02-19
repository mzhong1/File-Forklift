use crossbeam::channel::Receiver;
use postgres::Connection;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::error::ForkliftResult;
use crate::filesystem_ops::SyncOutcome;
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
    pub fn start(&self, conn: &Arc<Mutex<Option<Connection>>>) -> ForkliftResult<SyncStats> {
        let conn = conn.lock().unwrap();
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
                            post_update_files(file, &conn)?;
                        }
                        SyncOutcome::ChecksumUpdated(path, src_check, dest_check, size, update) => {
                            let file = Files::new(
                                format!("{:?}/{:?}", self.src_share, path),
                                src_check,
                                dest_check,
                                size,
                                update,
                            );
                            post_update_files(file, &conn)?;
                        }
                        _ => {}
                    }
                    stats.add_outcome(&x);
                    file_done = 0;
                }
                ProgressMessage::SendError(error) => {
                    post_forklift_err(&error, &conn)?;
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
            post_update_totalsync(stats.clone(), &conn)?;
        }
        self.progress_info.end(&stats);
        Ok(stats)
    }
}
