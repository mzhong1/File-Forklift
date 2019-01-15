//SyncStats
use crate::error::ForkliftResult;
use crate::filesystem::*;
use crate::filesystem_entry::Entry;
use crate::filesystem_ops::SyncOutcome;
use crate::progress_message::*;
use crate::progress_worker::*;
use crate::rsync_worker::*;
use crate::socket_node::*;
use crate::walk_worker::*;

use crossbeam::channel;
use rendezvous_hash::{DefaultNodeHasher, RendezvousNodes};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;

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
            SyncOutcome::FileCopied => self.copied += 1,
            SyncOutcome::UpToDate => self.up_to_date += 1,
            SyncOutcome::SymlinkUpdated => self.symlink_updated += 1,
            SyncOutcome::SymlinkCreated => self.symlink_created += 1,
            SyncOutcome::SymlinkSkipped => self.symlink_skipped += 1,
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

    pub fn create_contexts(
        val: FileSystemInitValues,
        (src_ip, dest_ip): (&str, &str),
        (src_share, dest_share): (&str, &str),
        level: u32,
    ) -> ForkliftResult<(
        NetworkContext,
        NetworkContext,
        NetworkContext,
        NetworkContext,
    )> {
        let (mut src_walk_context, mut dest_walk_context) = match val {
            FileSystemInitValues::Samba(wg, un, pw) => {
                let ctx = create_smb_context(&wg, &un, &pw, level)?;
                let dest_ctx = ctx.clone();
                (ctx, dest_ctx)
            }
            FileSystemInitValues::Nfs => {
                let src_ctx = create_nfs_context(src_ip, src_share, level)?;
                let dest_ctx = create_nfs_context(dest_ip, dest_share, level)?;
                (src_ctx, dest_ctx)
            }
        };
        let (mut src_sync_context, mut dest_sync_context) =
            (src_walk_context.clone(), dest_walk_context.clone());
        Ok((
            src_walk_context,
            dest_walk_context,
            src_sync_context,
            dest_sync_context,
        ))
    }

    // one Filesystem InitValues or 4 mut NetworkContexts?????
    //Note: technically NFS needs 2, SMB 1 Context, and clone them (4 times for Smb, once each for nfs)
    // DOES NFS HAVE SAME UID GID on either side????
    pub fn sync(
        self,
        val: FileSystemInitValues,
        (src_ip, dest_ip): (&str, &str),
        (src_share, dest_share): (&str, &str),
        (level, num_threads): (u32, u32),
        nodelist: Arc<Mutex<RendezvousNodes<SocketNode, DefaultNodeHasher>>>,
        my_node: SocketNode,
    ) -> ForkliftResult<()> {
        let (walker_output, rsync_input) = channel::unbounded::<Option<Entry>>();
        let (stat_output, progress_input) = channel::unbounded::<ProgressMessage>();
        let progress_output = stat_output.clone();

        //create contexts
        let (mut src_walk_context, mut dest_walk_context) = match val {
            FileSystemInitValues::Samba(wg, un, pw) => {
                let ctx = create_smb_context(&wg, &un, &pw, level)?;
                let dest_ctx = ctx.clone();
                (ctx, dest_ctx)
            }
            FileSystemInitValues::Nfs => {
                let src_ctx = create_nfs_context(src_ip, src_share, level)?;
                let dest_ctx = create_nfs_context(dest_ip, dest_share, level)?;
                (src_ctx, dest_ctx)
            }
        };
        let (mut src_sync_context, mut dest_sync_context) =
            (src_walk_context.clone(), dest_walk_context.clone());
        /*//create src + dest paths
        let src_path = format!("smb://{}/{}", src_ip, src_share);
        let dest_path = format!("smb://{}/{}", dest_ip, dest_share);
        let (source_path, dest_path) = match self.filesystem_type {
            FileSystemType::Samba => (Path::new(&src_path), Path::new(&dest_path)),
            FileSystemType::Nfs => (Path::new("/"), Path::new("/")),
        };*/

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads as usize)
            .build()
            .unwrap();

        //init walkers
        let src_path = self.source;
        let walk_worker = WalkWorker::new(&src_path, walker_output, stat_output, nodelist, my_node);
        let rsync_worker =
            RsyncWorker::new(&src_path, &self.destination, rsync_input, progress_output);
        let progress_worker = ProgressWorker::new(progress_input, self.progress_info);

        /*let walk_handle = thread::spawn(move || {
            walk_worker.s_walk(&src_path, &mut src_walk_context, &mut dest_walk_context);
        });*/
        let dest_path = self.destination;
        pool.install(|| {
            walk_worker.t_walk(
                &src_path,
                &dest_path,
                &mut src_walk_context,
                &mut dest_walk_context,
            )
        })?;
        let sync_handle = thread::spawn(move || {
            rsync_worker.start(&mut src_sync_context, &mut dest_sync_context);
        });
        let progress_handle = thread::spawn(move || {
            progress_worker.start();
        });

        //let walk_outcome = walk_handle.join();
        let sync_outcome = sync_handle.join();
        let progress_outcome = progress_handle.join();
        Ok(())
    }
}
