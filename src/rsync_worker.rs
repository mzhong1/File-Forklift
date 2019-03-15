use std::path::{Path, PathBuf};

use crossbeam::channel::Receiver;
use crossbeam::channel::Sender;
use log::*;
use rayon::*;

use crate::error::*;
use crate::filesystem::*;
use crate::filesystem_entry::Entry;
use crate::filesystem_ops::*;
use crate::postgres_logger::LogMessage;
use crate::progress_message::ProgressMessage;

#[derive(Clone)]
/// threaded worker handling Entry Processing for the rsync
pub struct RsyncWorker {
    /// source root path
    source: PathBuf,
    /// destination root path
    destination: PathBuf,
    /// source context
    src_context: ProtocolContext,
    /// destination context
    dest_context: ProtocolContext,
    /// input channel from WalkWorker
    pub input: Receiver<Option<Entry>>,
    /// channel to send progress
    progress_output: Sender<ProgressMessage>,
    /// channel to send logs to postgres
    pub log_output: Sender<LogMessage>,
}

impl RsyncWorker {
    /// create a new RsyncWorker
    pub fn new(
        source: &Path,
        destination: &Path,
        src_context: ProtocolContext,
        dest_context: ProtocolContext,
        input: Receiver<Option<Entry>>,
        progress_output: Sender<ProgressMessage>,
        log_output: Sender<LogMessage>,
    ) -> RsyncWorker {
        RsyncWorker {
            source: source.to_path_buf(),
            destination: destination.to_path_buf(),
            src_context,
            dest_context,
            input,
            progress_output,
            log_output,
        }
    }

    /// Process the entries sent through input channel
    pub fn start(self, pool: &ThreadPool) -> ForkliftResult<()> {
        let id = get_index_or_rand(pool);
        for entry in self.input.iter() {
            let input_entry = match entry {
                Some(e) => e,
                None => break,
            };
            let sync_outcome = self.sync(&input_entry)?;
            let len = self.input.len();
            debug!("Sync Thread {:?} Outcome: {:?} Num left {:?}", id, sync_outcome, len,);
            let progress = ProgressMessage::DoneSyncing(sync_outcome);
            if let Err(e) = self.progress_output.send(progress) {
                return Err(ForkliftError::CrossbeamChannelError(format!(
                    "Error: {:?}, unable to send progress",
                    e
                )));
            };
            trace!("rec len {:?}", len);
        }
        Ok(())
    }

    /// process an Entry according to rsync rules
    fn sync(&self, src_entry: &Entry) -> ForkliftResult<SyncOutcome> {
        let rel_path = get_rel_path(&src_entry.path(), &self.source)?;
        let dest_path = &self.destination.join(&rel_path);
        let (src_context, dest_context) = (&self.src_context, &self.dest_context);
        make_dir_all(
            &src_entry.path(),
            &dest_path,
            &self.destination,
            src_context,
            dest_context,
            &self.log_output,
        )?;
        let dest_entry = Entry::new(&dest_path, dest_context);
        let mut outcome = sync_entry(
            src_entry,
            &dest_entry,
            src_context,
            dest_context,
            &self.progress_output,
            &self.log_output,
        )?;
        let is_dir = match src_entry.is_dir() {
            Some(d) => d,
            None => {
                return Err(ForkliftError::FSError("src entry does not exist".to_string()));
            }
        };
        if !is_dir {
            let temp_outcome = copy_permissions(
                src_entry,
                &dest_entry,
                src_context,
                dest_context,
                &self.log_output,
            )?;
            let current_outcome = outcome.clone();
            outcome = match (outcome, temp_outcome) {
                (SyncOutcome::UpToDate, SyncOutcome::PermissionsUpdated) => {
                    SyncOutcome::PermissionsUpdated
                }
                (..) => current_outcome,
            }
        }
        Ok(outcome)
    }
}
