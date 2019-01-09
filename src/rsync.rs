//SyncStats
use crate::error::ForkliftResult;
use crate::filesystem::FileSystemType;
use crate::filesystem_entry::Entry;
use crate::filesystem_ops::SyncOutcome;
use crate::progress_message::*;
use crate::walk_worker::*;

use crossbeam::channel;
use std::path::{Path, PathBuf};

#[derive(Default, Debug, Clone)]
pub struct SyncStats {
    /// total number of files in the source
    pub tot_files: u64,
    /// Total size of all files in the source
    pub tot_size: usize,
    /// Number of files transferred from source
    /// to dest (should match tot_files if no error)
    pub num_synced: u64,
    /// number of files that are up to date (and therefore
    /// need no copies or modification)
    pub up_to_date: u64,
    /// the total number of files that were copied
    pub copied: u64,
    /// number of symlinks created in dest
    pub symlink_created: u64,
    /// number of symlinks updated in dest
    pub symlink_updated: u64,
    /// number of files for which the permissions were updated
    pub permissions_update: u64,
    /// the number of files where dest file contents were updated
    pub checksum_updated: u64,
    /// the number of directories where dest directory was created
    pub directory_created: u64,
    /// the number of directories where the dest directory permissions were updated
    pub directory_updated: u64,
}

impl SyncStats {
    pub fn new() -> SyncStats {
        SyncStats {
            tot_files: 0,
            tot_size: 0,
            num_synced: 0,
            up_to_date: 0,
            copied: 0,
            symlink_created: 0,
            symlink_updated: 0,
            permissions_update: 0,
            checksum_updated: 0,
            directory_created: 0,
            directory_updated: 0,
        }
    }
    pub fn add_outcome(&mut self, outcome: &SyncOutcome) {
        self.num_synced += 1;
        match outcome {
            SyncOutcome::FileCopied => self.copied += 1,
            SyncOutcome::UpToDate => self.up_to_date += 1,
            SyncOutcome::SymlinkUpdated => self.symlink_updated += 1,
            SyncOutcome::SymlinkCreated => self.symlink_created += 1,
            SyncOutcome::PermissionsUpdated => self.permissions_update += 1,
            SyncOutcome::ChecksumUpdated => self.checksum_updated += 1,
            SyncOutcome::DirectoryUpdated => self.directory_updated += 1,
            SyncOutcome::DirectoryCreated => self.directory_created += 1,
        }
    }
}

pub struct Rsyncer {
    source: PathBuf,
    destination: PathBuf,
    filesystem_type: FileSystemType,
    progress_info: Box<ProgressInfo + Send>,
}

impl Rsyncer {
    pub fn new(
        source: &Path,
        destination: &Path,
        filesystem_type: FileSystemType,
        progress_info: Box<ProgressInfo + Send>,
    ) -> Rsyncer {
        Rsyncer {
            source: source.to_path_buf(),
            destination: destination.to_path_buf(),
            filesystem_type,
            progress_info,
        }
    }

    pub fn sync(self) -> ForkliftResult<()> {
        let (walker_output, rsync_input) = channel::unbounded::<Entry>();
        let (stat_output, progress_input) = channel::unbounded::<ProgressMessage>();
        let progress_output = walker_output.clone();

        //depending on filesystem, create src/dest contexts
        //create init function???
        //

        //let walk_worker = WalkWorker::new(source: &Path, entry_output: Sender<Option<Entry>>, progress_output: Sender<ProgressMessage>)
        Ok(())
    }
}
