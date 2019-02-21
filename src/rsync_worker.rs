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
        input: Receiver<Option<Entry>>,
        progress_output: Sender<ProgressMessage>,
        log_output: Sender<LogMessage>,
    ) -> RsyncWorker {
        RsyncWorker {
            source: source.to_path_buf(),
            destination: destination.to_path_buf(),
            input,
            progress_output,
            log_output,
        }
    }

    /// Process the entries sent through input channel
    pub fn start(
        self,
        contexts: &mut Vec<(ProtocolContext, ProtocolContext)>,
        pool: &ThreadPool,
    ) -> ForkliftResult<()> {
        let id = get_index_or_rand(pool);
        let index = id % contexts.len();
        let (mut src, mut dest) = match contexts.get(index) {
            Some((s, d)) => (s.clone(), d.clone()),
            None => {
                error!("unable to retrieve contexts");
                return Err(ForkliftError::FSError("Unable to retrieve contexts".to_string()));
            }
        };

        for entry in self.input.iter() {
            let e = match entry {
                Some(e) => e,
                None => break,
            };
            let sync_outcome = self.sync(&e, &mut src, &mut dest)?;
            debug!(
                "Sync Thread {:?} Outcome: {:?} Num left {:?}",
                id,
                sync_outcome,
                self.input.len(),
            );
            let progress = ProgressMessage::DoneSyncing(sync_outcome);
            if let Err(e) = self.progress_output.send(progress) {
                return Err(ForkliftError::CrossbeamChannelError(format!(
                    "Error: {:?}, unable to send progress",
                    e
                )));
            };
            trace!("rec len {:?}", self.input.len());
        }
        Ok(())
    }

    /// process an Entry according to rsync rules
    fn sync(
        &self,
        src_entry: &Entry,
        src_context: &mut ProtocolContext,
        dest_context: &mut ProtocolContext,
    ) -> ForkliftResult<SyncOutcome> {
        let rel_path = get_rel_path(&src_entry.path(), &self.source)?;
        let dest_path = &self.destination.join(&rel_path);
        let mut src_context = src_context;
        let mut dest_context = dest_context;
        make_dir_all(
            &src_entry.path(),
            &dest_path,
            &self.destination,
            &mut src_context,
            &mut dest_context,
        )?;
        let dest_entry = Entry::new(&dest_path, &dest_context);
        let mut outcome = sync_entry(
            &src_entry,
            &dest_entry,
            src_context,
            dest_context,
            &self.progress_output,
            &self.log_output,
        )?;
        let dir = match src_entry.is_dir() {
            Some(d) => d,
            None => true,
        };
        if !dir {
            let temp_outcome =
                copy_permissions(&src_entry, &dest_entry, src_context, dest_context)?;
            let c = outcome.clone();
            outcome = match (outcome, temp_outcome) {
                (SyncOutcome::UpToDate, SyncOutcome::PermissionsUpdated) => {
                    SyncOutcome::PermissionsUpdated
                }
                (..) => c,
            }
        }
        Ok(outcome)
    }
}
