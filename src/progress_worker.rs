use crossbeam::channel::Receiver;
use std::time::Instant;

use crate::progress_message::*;
use crate::rsync::SyncStats;

pub struct ProgressWorker {
    input: Receiver<ProgressMessage>,
    progress_info: Box<ProgressInfo + Send>,
}

impl ProgressWorker {
    pub fn new(
        input: Receiver<ProgressMessage>,
        progress_info: Box<ProgressInfo + Send>,
    ) -> ProgressWorker {
        ProgressWorker {
            input,
            progress_info,
        }
    }

    pub fn start(&self) -> SyncStats {
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
                    stats.tot_files = num_files;
                    stats.tot_size = total_size;
                }
                ProgressMessage::StartSync(x) => {
                    self.progress_info.new_file(&x);
                    current_file = x;
                    index += 1;
                }
                ProgressMessage::DoneSyncing(x) => {
                    self.progress_info.done_syncing();
                    stats.add_outcome(&x);
                    file_done = 0;
                }
                ProgressMessage::Syncing { done, size, .. } => {
                    file_done += done;
                    total_done += done;
                    let elapsed = now.elapsed().as_secs() as usize;
                    let eta = ((elapsed * stats.tot_size) / total_done) - elapsed;
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
        }
        self.progress_info.end(&stats);
        stats
    }
}
