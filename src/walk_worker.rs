use crate::error::*;
use crate::filesystem::*;
use crate::filesystem_entry::Entry;
use crate::filesystem_ops::*;
use crate::progress_message::ProgressMessage;
use crate::socket_node::*;

use crossbeam::channel::Sender;
use lazy_static::*;
use log::*;
use rayon::*;
use rendezvous_hash::{DefaultNodeHasher, RendezvousNodes};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

lazy_static! {
    pub static ref this: PathBuf = Path::new(".").to_path_buf();
    pub static ref parent: PathBuf = Path::new("..").to_path_buf();
}

/// threaded worker to walk through a filesystem
pub struct WalkWorker {
    /// source path
    source: PathBuf,
    /// destination root path
    destination: PathBuf,
    /// list of contexts
    contexts: Vec<(ProtocolContext, ProtocolContext)>,
    /// Current Node to determine what is processed
    node: SocketNode,
    /// Nodes to calculate entry processor
    nodes: Arc<Mutex<RendezvousNodes<SocketNode, DefaultNodeHasher>>>,
    /// channels to send entries to processors
    entry_outputs: Vec<Sender<Option<Entry>>>,
    /// channel to send progress information
    progress_output: Sender<ProgressMessage>,
}

impl WalkWorker {
    /// create a new WalkWorker
    pub fn new(
        source: &Path,
        destination: &Path,
        contexts: Vec<(ProtocolContext, ProtocolContext)>,
        node: SocketNode,
        nodes: Arc<Mutex<RendezvousNodes<SocketNode, DefaultNodeHasher>>>,
        entry_outputs: Vec<Sender<Option<Entry>>>,
        progress_output: Sender<ProgressMessage>,
    ) -> WalkWorker {
        WalkWorker {
            entry_outputs,
            progress_output,
            contexts,
            source: source.to_path_buf(),
            destination: destination.to_path_buf(),
            nodes,
            node,
        }
    }

    /// stop all senders, ending walk
    pub fn stop(&self) -> ForkliftResult<()> {
        for output in self.entry_outputs.iter() {
            // Stop all the senders
            if output.send(None).is_err() {
                return Err(ForkliftError::CrossbeamChannelError(
                    "Error, channel disconnected, unable to stop rsync_worker".to_string(),
                ));
            }
        }
        Ok(())
    }

    // grab a sender handler and send in the path
    // Find the sender with the smallest length of channel
    // Send the path over to that to be sync'd
    // Assuming they are all unbounded
    pub fn do_work(&self, entry: Option<Entry>) -> ForkliftResult<()> {
        let sender = match self.entry_outputs.get(0) {
            Some(s) => s,
            None => {
                return Err(ForkliftError::CrossbeamChannelError(
                    "Empty channel vector!".to_string(),
                ));
            }
        };
        //get sender with least number of messages pending
        let mut min = sender.len();
        let mut index = 0;
        for (i, sender) in self.entry_outputs.iter().enumerate() {
            if sender.len() < min {
                min = sender.len();
                index = i;
            }
        }
        let sender = match self.entry_outputs.get(index) {
            Some(s) => s,
            None => {
                return Err(ForkliftError::CrossbeamChannelError(
                    "Empty channel vector in walk_worker!".to_string(),
                ));
            }
        };
        if let Err(e) = sender.send(entry) {
            return Err(ForkliftError::CrossbeamChannelError(format!(
                "Error {:?}, Unable to send entry to rsync_worker",
                e
            )));
        };
        Ok(())
    }

    /// threaded, recursive filetree walker
    pub fn t_walk(&self, path: &Path, pool: &ThreadPool) -> ForkliftResult<()> {
        rayon::scope(|spawner| {
            let index = get_index_or_rand(pool) % self.contexts.len();
            debug!("{:?}", index);
            let (src_context, dest_context) = match self.contexts.get(index) {
                Some((src, dest)) => (src, dest),
                None => {
                    return Err(ForkliftError::FSError("Unable to retrieve contexts".to_string()));
                }
            };
            let mut check_paths: Vec<PathBuf> = vec![];
            let check_path = self.get_check_path(&path)?;
            let check = exist(&check_path, dest_context);
            let dir = src_context.opendir(&path)?;
            for entrytype in dir {
                let entry = match entrytype {
                    Ok(f) => f,
                    Err(e) => {
                        return Err(e);
                    }
                };
                let file_path = entry.path();
                if file_path != this.as_path() && file_path != parent.as_path() {
                    let newpath = path.join(&file_path);
                    self.send_file(&newpath, src_context)?;
                    if let Some(true) = is_dir(&newpath, &entry) {
                        spawner.spawn(|_| {
                            let newpath = newpath;
                            if let Err(e) = self.t_walk(&newpath, &pool) {
                                let mess = ProgressMessage::SendError(ForkliftError::FSError(
                                    format!("Error {:?}, Unable to recursively call", e),
                                ));
                                self.progress_output.send(mess).expect("Unable to send progress")
                            }
                        });
                    }
                    if check {
                        let check_path = check_path.join(&file_path);
                        check_paths.push(check_path);
                    }
                }
            }
            // check through dest files
            let check_path = self.get_check_path(&path)?;
            self.check_and_remove((check, &mut check_paths), (&check_path, dest_context))?;
            Ok(())
        })?;
        Ok(())
    }

    /// send a file to the rsync worker
    fn send_file(&self, path: &Path, context: &ProtocolContext) -> ForkliftResult<bool> {
        let meta = self.process_file(path, context)?;
        if let Some(meta) = meta {
            if let Err(e) = self
                .progress_output
                .send(ProgressMessage::Todo { num_files: 1, total_size: meta.size() as usize })
            {
                return Err(ForkliftError::CrossbeamChannelError(format!(
                    "Error: {:?}, unable to send progress",
                    e
                )));
            };
            return Ok(true);
        }
        Ok(false)
    }
    /// linear walking loop
    fn walk_loop(
        &self,
        (path, stack): (&Path, &mut Vec<PathBuf>),
        (check, check_path, check_paths): (bool, &Path, &mut Vec<PathBuf>),
        (dir, src_context): (DirectoryType, &ProtocolContext),
    ) -> ForkliftResult<u64> {
        let mut total_files = 0;
        for entrytype in dir {
            let entry = entrytype?;
            let file_path = entry.path();
            if file_path != this.as_path() && file_path != parent.as_path() {
                let newpath = path.join(&file_path);
                //file exists?
                if self.send_file(&newpath, src_context)? {
                    total_files += 1;
                };
                if let Some(true) = is_dir(&newpath, &entry) {
                    stack.push(newpath);
                }
                if check {
                    let check_path = check_path.join(file_path);
                    check_paths.push(check_path);
                }
            }
        }
        Ok(total_files)
    }

    /// Linear filesystem walker
    pub fn s_walk(&self) -> ForkliftResult<()> {
        let (src_context, dest_context) = match self.contexts.get(0) {
            Some((src, dest)) => (src, dest),
            None => {
                return Err(ForkliftError::FSError("Unable to retrieve contexts".to_string()));
            }
        };
        let mut num_files = 0;
        let mut stack: Vec<PathBuf> = vec![self.source.clone()];
        loop {
            let check: bool;
            let mut check_paths: Vec<PathBuf> = vec![];
            match stack.pop() {
                Some(path) => {
                    let check_path = self.get_check_path(&path)?;
                    check = exist(&check_path, dest_context);
                    let dir = src_context.opendir(&path)?;
                    num_files += self.walk_loop(
                        (&path, &mut stack),
                        (check, &check_path, &mut check_paths),
                        (dir, src_context),
                    )?;
                    // check through dest files
                    self.check_and_remove((check, &mut check_paths), (&check_path, dest_context))?;
                }
                None => {
                    debug!("Total number of files sent {:?}", num_files);
                    break;
                }
            }
        }
        Ok(())
    }
    /// get the destination path to check against
    fn get_check_path(&self, source_path: &Path) -> ForkliftResult<PathBuf> {
        let rel_path = get_rel_path(&source_path, &self.source)?;
        Ok(self.destination.join(rel_path))
    }

    /// check if path exists and remove from list of paths in directory
    fn check_and_remove(
        &self,
        (check, check_paths): (bool, &mut Vec<PathBuf>),
        (check_path, dest_context): (&Path, &ProtocolContext),
    ) -> ForkliftResult<()> {
        // check through dest files
        if check {
            let dir = dest_context.opendir(&check_path)?;
            for entrytype in dir {
                let entry = entrytype?;
                let file_path = entry.path();
                if file_path != this.as_path() && file_path != parent.as_path() {
                    let newpath = check_path.join(file_path);
                    if newpath == Path::new("/removeeee") {
                        println!("{:?}", &newpath);
                    }
                    if !contains_and_remove(check_paths, &newpath) {
                        match entry.filetype() {
                            GenericFileType::Directory => {
                                trace!("call remove_dir: {:?}", &newpath);
                                remove_dir(&newpath, dest_context)?;
                            }
                            _ => {
                                debug!("remove: {:?}", &newpath);
                                dest_context.unlink(&newpath)?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// process a file, determining whether to send it to rsync_worker to be synced or skipped
    fn process_file(
        &self,
        entry: &Path,
        src_context: &ProtocolContext,
    ) -> ForkliftResult<Option<Stat>> {
        let node = match self.nodes.lock() {
            Ok(e) => {
                let mut list = e;
                trace!("{:?}", list.calc_candidates(&entry.to_string_lossy()).collect::<Vec<_>>());
                match list.calc_candidates(&entry.to_string_lossy()).nth(0) {
                    Some(p) => *p,
                    None => {
                        return Err(ForkliftError::FSError("calc candidates failed".to_string()));
                    }
                }
            }
            Err(_) => {
                return Err(ForkliftError::FSError("failed to lock".to_string()));
            }
        };
        if node == self.node {
            let src_entry = Entry::new(entry, src_context);
            let metadata = match src_entry.metadata() {
                Some(stat) => stat,
                None => {
                    return Ok(None);
                }
            };
            self.do_work(Some(src_entry))?;
            return Ok(Some(metadata));
        }
        Ok(None)
    }
}

/// check if directory entry is a directory
fn is_dir(path: &Path, entry: &DirEntryType) -> Option<bool> {
    match entry.filetype() {
        GenericFileType::Directory => {
            debug!("dir: {:?}", path);
            Some(true)
        }
        GenericFileType::File => {
            debug!("file: {:?}", path);
            Some(false)
        }
        GenericFileType::Link => {
            debug!("link: {:?}", path);
            Some(false)
        }
        GenericFileType::Other => None,
    }
}

/// check if path is in check_paths, and remove if so
fn contains_and_remove(check_paths: &mut Vec<PathBuf>, check_path: &Path) -> bool {
    for (count, source_path) in check_paths.iter().enumerate() {
        if source_path == check_path {
            check_paths.remove(count);
            return true;
        }
    }
    false
}

/// recursively remove a directory in destination that is not in source
fn remove_dir(path: &Path, dest_context: &ProtocolContext) -> ForkliftResult<()> {
    let mut stack: Vec<PathBuf> = vec![(*path).to_path_buf()];
    let mut remove_stack: Vec<PathBuf> = vec![(*path).to_path_buf()];
    while let Some(p) = stack.pop() {
        let dir = dest_context.opendir(&p)?;
        for entrytype in dir {
            let entry = match entrytype {
                Ok(e) => e,
                Err(e) => {
                    return Err(e);
                }
            };
            let file_path = entry.path();
            if file_path != this.as_path() && file_path != parent.as_path() {
                let newpath = p.join(&file_path);
                debug!("remove: {:?}", &newpath);
                match is_dir(&newpath, &entry) {
                    Some(true) => {
                        stack.push(newpath.clone());
                        remove_stack.push(newpath);
                    }
                    Some(false) => {
                        dest_context.unlink(&newpath)?;
                    }
                    None => {}
                }
            }
        }
    }
    while !remove_stack.is_empty() {
        let dir = match remove_stack.pop() {
            Some(e) => e,
            None => {
                return Err(ForkliftError::FSError("remove stack should not be empty!".to_string()));
            }
        };
        dest_context.rmdir(&dir)?;
    }
    Ok(())
}
