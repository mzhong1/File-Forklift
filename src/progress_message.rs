use crate::error::ForkliftError;
use crate::filesystem_ops::SyncOutcome;
use crate::rsync::SyncStats;

/// enum holding progress messages
#[derive(Debug)]
pub enum ProgressMessage {
    /// wrapper for SyncOutcomes
    DoneSyncing(SyncOutcome),
    /// start syncing a file
    StartSync(String),
    /// update number of files to sync + size
    Todo { num_files: u64, tot_size: usize },
    /// Error message
    SendError(ForkliftError),
    /// sync in progress
    CheckSyncing { description: String, size: usize, done: usize },
    /// end the Sync
    EndSync,
}

/// Store the progress of the rsync
#[derive(Clone, Debug)]
pub struct Progress {
    /// Name of the file being transferred
    pub current_file: String,
    /// Number of bytes transfered for the current file
    pub file_done: usize,
    /// Size of the current file (in bytes)
    pub file_size: usize,
    /// Number of bytes transfered since the start
    pub total_done: usize,
    /// Estimated total size of the transfer (this may change during transfer)
    pub total_size: usize,
    /// Index of the current file in the list of all files to transfer
    pub index: usize,
    /// Total number of files to transfer
    pub num_files: usize,
    /// Estimated time remaining for the transfer, in seconds
    pub eta: usize,
}

// Trait for implementing rusync progress details
pub trait ProgressInfo {
    /// A new transfer has begun from the `source` directory to the `destination`
    /// directory
    #[allow(unused_variables)]
    fn start(&self, source: &str, destination: &str) {}

    /// A new file named `name` is being transfered
    #[allow(unused_variables)]
    fn new_file(&self, name: &str) {}

    /// The file transfer is done
    #[allow(unused_variables)]
    fn done_syncing(&self) {}

    /// Callback for the detailed progress
    #[allow(unused_variables)]
    fn progress(&self, progress: &Progress) {}

    /// The transfer between `source` and `destination` is done. Details
    /// of the transfer in the Stats struct
    #[allow(unused_variables)]
    fn end(&self, stats: &SyncStats) {}
}
