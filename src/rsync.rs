//SyncStats
use crate::error::{ForkliftError, ForkliftResult};
use crate::filesystem::*;
use crate::filesystem_entry::Entry;
use crate::filesystem_ops::SyncOutcome;
use crate::progress_message::*;
use crate::progress_worker::*;
use crate::rsync_worker::*;
use crate::socket_node::*;
use crate::walk_worker::*;
use crate::LogMessage;

use crossbeam::channel;
use crossbeam::channel::Sender;
use log::*;
use rendezvous_hash::{DefaultNodeHasher, RendezvousNodes};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

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
    /// number of symlinks skipped in dest
    pub symlink_skipped: u64,
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
            symlink_skipped: 0,
            permissions_update: 0,
            checksum_updated: 0,
            directory_created: 0,
            directory_updated: 0,
        }
    }
    pub fn add_outcome(&mut self, outcome: &SyncOutcome) {
        self.num_synced += 1;
        match outcome {
            SyncOutcome::FileCopied(..) => self.copied += 1,
            SyncOutcome::UpToDate => self.up_to_date += 1,
            SyncOutcome::SymlinkUpdated => self.symlink_updated += 1,
            SyncOutcome::SymlinkCreated => self.symlink_created += 1,
            SyncOutcome::SymlinkSkipped => self.symlink_skipped += 1,
            SyncOutcome::PermissionsUpdated => self.permissions_update += 1,
            SyncOutcome::ChecksumUpdated(..) => self.checksum_updated += 1,
            SyncOutcome::DirectoryUpdated => self.directory_updated += 1,
            SyncOutcome::DirectoryCreated => self.directory_created += 1,
        }
    }
}

pub struct Rsyncer {
    /// share protocol to usize
    filesystem_type: FileSystemType,
    /// console ouput functions
    progress_info: Box<ProgressInfo + Send + Sync>,
    /// channe to send postgres logs
    postgres_output: Sender<LogMessage>,
    /// source root path
    source: PathBuf,
    /// destination root path,
    destination: PathBuf,
}

impl Rsyncer {
    pub fn new(
        filesystem_type: FileSystemType,
        progress_info: Box<ProgressInfo + Send + Sync>,
        postgres_output: Sender<LogMessage>,
        source: PathBuf,
        destination: PathBuf,
    ) -> Rsyncer {
        Rsyncer {
            filesystem_type,
            progress_info,
            postgres_output,
            source,
            destination,
        }
    }

    pub fn sync(
        self,
        (src_ip, dest_ip): (&str, &str),
        (src_share, dest_share): (&str, &str),
        (level, num_threads): (u32, u32),
        (workgroup, username, password): (String, String, String),
        nodelist: Arc<Mutex<RendezvousNodes<SocketNode, DefaultNodeHasher>>>,
        my_node: SocketNode,
    ) -> ForkliftResult<()> {
        let mut send_handles: Vec<Sender<Option<Entry>>> = Vec::new();

        let mut syncers: Vec<RsyncWorker> = Vec::new();

        let (send_prog, rec_prog) = channel::unbounded::<ProgressMessage>();
        for _ in 0..num_threads {
            let (send_e, rec_e) = channel::unbounded();
            send_handles.push(send_e);
            let sync_progress = send_prog.clone();
            syncers.push(RsyncWorker::new(
                self.source.as_path(),
                self.destination.as_path(),
                rec_e,
                sync_progress,
            ));
        }
        let mut contexts: Vec<(NetworkContext, NetworkContext)> = Vec::new();
        let mut sync_contexts: Vec<(NetworkContext, NetworkContext)> = Vec::new();
        let s = init_samba(workgroup, username, password, level)?;
        for _ in 0..num_threads {
            match self.filesystem_type {
                FileSystemType::Samba => {
                    let (sctx, dctx) = (
                        NetworkContext::Samba(Box::new(s.clone())),
                        NetworkContext::Samba(Box::new(s.clone())),
                    );
                    contexts.push((sctx, dctx));
                    let (sctx, dctx) = (
                        NetworkContext::Samba(Box::new(s.clone())),
                        NetworkContext::Samba(Box::new(s.clone())),
                    );
                    sync_contexts.push((sctx, dctx));
                }
                FileSystemType::Nfs => {
                    let (sctx, dctx) = (
                        create_nfs_context(src_ip, src_share, level)?,
                        create_nfs_context(dest_ip, dest_share, level)?,
                    );

                    contexts.push((sctx.clone(), dctx.clone()));
                    sync_contexts.push((sctx, dctx));
                }
            }
        }
        let send_prog_thread = send_prog.clone();
        let walk_worker = WalkWorker::new(
            self.source.as_path(),
            send_handles,
            send_prog,
            nodelist,
            my_node,
        );
        let progress_worker =
            ProgressWorker::new(rec_prog, self.progress_info, src_share.to_string());
        let c = self.postgres_output.clone();
        rayon::spawn(move || {
            progress_worker.start(&c).unwrap();
        });
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads as usize)
            .breadth_first()
            .build()
            .unwrap();

        if num_threads == 1 {
            let (mut fs, mut destfs) = contexts[0].clone();
            walk_worker.s_walk(self.source.as_path(), &mut fs, &mut destfs)?;
            walk_worker.stop()?;
        }
        let src_path = self.source.as_path();
        let dest_path = self.destination.as_path();
        pool.install(|| {
            if num_threads > 1 {
                match walk_worker.t_walk(dest_path, src_path, &mut contexts, &pool) {
                    Ok(_) => {}
                    Err(e) => {
                        return Err(e);
                    }
                }
                walk_worker.stop()?;
            }

            rayon::scope(|spawner| {
                for syncer in syncers {
                    spawner.spawn(|_| {
                        let input = syncer.input.clone();
                        match syncer.start(&mut sync_contexts.clone(), &pool) {
                            Ok(_) => {
                                debug!(
                                    "Syncer Stopped, Thread {:?}, num left {:?}",
                                    pool.current_thread_index(),
                                    input.len()
                                );
                            }
                            Err(e) => {
                                error!("Error: {:?}", e);
                                let mess = ProgressMessage::SendError(e);
                                send_prog_thread.send(mess).unwrap();
                            }
                        };
                    });
                }
            });
            Ok(())
        })?;
        if self.postgres_output.send(LogMessage::End).is_err() {
            return Err(ForkliftError::CrossbeamChannelError(
                "Channel to heartbeat is broken!".to_string(),
            ));
        }
        Ok(())
    }
}
