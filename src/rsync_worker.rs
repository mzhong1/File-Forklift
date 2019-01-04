use std::path::{Path, PathBuf};

use crossbeam::channel::Receiver;
use crossbeam::channel::Sender;

use crate::error::*;
use crate::filesystem::*;
use crate::filesystem_entry::Entry;
use crate::filesystem_ops::*;
use crate::progress_message::ProgressMessage;

pub struct RsyncWorker {
    input: Receiver<Option<Entry>>,
    output: Sender<ProgressMessage>,
    source: PathBuf,
    destination: PathBuf,
}

impl RsyncWorker {
    pub fn new(
        source: &Path,
        destination: &Path,
        input: Receiver<Option<Entry>>,
        output: Sender<ProgressMessage>,
    ) -> RsyncWorker {
        RsyncWorker {
            source: source.to_path_buf(),
            destination: destination.to_path_buf(),
            input,
            output,
        }
    }

    pub fn start(
        self,
        src_context: &mut NetworkContext,
        dest_context: &mut NetworkContext,
    ) -> ForkliftResult<()> {
        for entry in self.input.iter() {
            let e = match entry {
                Some(e) => e,
                None => {
                    break;
                }
            };
            let sync_outcome = self.sync(&e, src_context, dest_context)?;
            println!("Outcome: {:?}", sync_outcome);
            let progress = ProgressMessage::DoneSyncing(sync_outcome);
            match self.output.send(progress) {
                Ok(_) => {}
                Err(e) => {
                    return Err(ForkliftError::FSError(format!(
                        "Error: {:?}, unable to send progress",
                        e
                    )));
                }
            };
            println!("rec len {:?}", self.input.len());
        }
        Ok(())
    }

    fn sync(
        &self,
        src_entry: &Entry,
        src_context: &mut NetworkContext,
        dest_context: &mut NetworkContext,
    ) -> ForkliftResult<SyncOutcome> {
        let rel_path = get_rel_path(&src_entry.path(), &self.source)?;
        let dest_path = &self.destination.join(&rel_path);
        make_dir_all(
            &dest_path,
            &src_entry.path(),
            &self.destination,
            src_context,
            dest_context,
        )?;
        let dest_entry = Entry::new(&dest_path, &dest_context);
        let mut outcome = sync_entry(&src_entry, src_context, &dest_entry, dest_context)?;
        if !src_entry.is_dir().unwrap() {
            let temp_outcome =
                copy_permissions(&src_entry, src_context, &dest_entry, dest_context)?;
            let c = outcome.clone();
            outcome = match (outcome, temp_outcome) {
                (SyncOutcome::UpToDate, SyncOutcome::PermissionsUpdated) => {
                    SyncOutcome::PermissionsUpdated
                }
                (_, _) => c,
            }
        }
        Ok(outcome)
    }
}