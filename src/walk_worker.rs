use crate::error::*;
use crate::filesystem::*;
use crate::filesystem_entry::Entry;
use crate::filesystem_ops::*;
use crate::progress_message::ProgressMessage;
use crate::socket_node::*;

use crossbeam::channel::Sender;
use log::*;
use rayon::*;
use rendezvous_hash::{DefaultNodeHasher, RendezvousNodes};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub struct WalkWorker {
    entry_outputs: Vec<Sender<Option<Entry>>>,
    progress_output: Sender<ProgressMessage>,
    source: PathBuf,
    nodes: Arc<Mutex<RendezvousNodes<SocketNode, DefaultNodeHasher>>>,
    node: SocketNode,
}

impl WalkWorker {
    pub fn new(
        source: &Path,
        entry_outputs: Vec<Sender<Option<Entry>>>,
        progress_output: Sender<ProgressMessage>,
        nodes: Arc<Mutex<RendezvousNodes<SocketNode, DefaultNodeHasher>>>,
        node: SocketNode,
    ) -> WalkWorker {
        WalkWorker {
            entry_outputs,
            progress_output,
            source: source.to_path_buf(),
            nodes,
            node,
        }
    }

    pub fn stop(&self) -> ForkliftResult<()> {
        for s in self.entry_outputs.iter() {
            // Stop all the senders
            if s.send(None).is_err() {
                error!("Unable to stop");
                return Err(ForkliftError::FSError(
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
                return Err(ForkliftError::FSError("Empty channel vector!".to_string()));
            }
        };
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
                return Err(ForkliftError::FSError("Empty channel vector!".to_string()));
            }
        };
        if let Err(_e) = sender.send(entry) {
            error!("Unable to send Entry");
            return Err(ForkliftError::FSError("Unable to send entry".to_string()));
        };
        Ok(())
    }

    pub fn t_walk(
        &self,
        root_path: &Path,
        path: &Path,
        contexts: &mut Vec<(NetworkContext, NetworkContext)>,
        pool: &ThreadPool,
    ) -> ForkliftResult<()> {
        rayon::scope(|spawner| {
            let id = get_index_or_rand(pool);
            debug!("{:?}", id);
            let index = id % contexts.len();

            let (mut src_context, mut dest_context) = match contexts.get(index) {
                Some((s, d)) => (s.clone(), d.clone()),
                None => {
                    error!("unable to retrieve contexts");
                    return Err(ForkliftError::FSError(
                        "Unable to retrieve contexts".to_string(),
                    ));
                }
            };

            let (mut num_files, mut total_size) = (0, 0);
            let (this, parent) = (Path::new("."), Path::new(".."));
            let check: bool;
            let mut check_paths: Vec<PathBuf> = vec![];
            let check_path = self.get_check_path(&path, root_path)?;
            check = exist(&check_path, &mut dest_context);
            let dir = src_context.opendir(&path)?;

            for entrytype in dir {
                let entry = match entrytype {
                    Ok(f) => f,
                    Err(e) => {
                        error!("Error, non-unicode character in file path");
                        return Err(e);
                    }
                };
                let file_path = entry.path();

                if file_path != this && file_path != parent {
                    let newpath = path.join(&file_path);
                    let meta =
                        self.process_file(&newpath, &mut src_context, &self.nodes.clone())?;
                    if let Some(meta) = meta {
                        debug!("Sent: {:?}", &file_path);
                        num_files += 1;
                        total_size += meta.size() as usize;
                        //why is this not a forkliftResult? because threading sucks
                        if let Err(e) = self.progress_output.send(ProgressMessage::Todo {
                            num_files,
                            total_size: total_size as usize,
                        }) {
                            error!("Error {:?} unable to send progress", e);
                            return Err(ForkliftError::FSError(
                                "unable to send progress".to_string(),
                            ));
                        }
                    }
                    match entry.filetype() {
                        GenericFileType::Directory => {
                            debug!("dir: {:?}", &newpath);
                            let loop_contexts = contexts.clone();
                            spawner.spawn(|_| {
                                let mut contexts = loop_contexts;
                                let newpath = newpath;
                                if let Err(_e) =
                                    self.t_walk(&root_path, &newpath, &mut contexts, &pool)
                                {
                                    error!("Unable to recursively call");
                                }
                            });
                        }
                        GenericFileType::File => {
                            debug!("file: {:?}", &newpath);
                        }
                        GenericFileType::Link => {
                            debug!("link: {:?}", &newpath);
                        }
                        GenericFileType::Other => {}
                    }
                    if check {
                        let check_path = check_path.join(&file_path);
                        check_paths.push(check_path);
                    }
                }
            }
            // check through dest files
            self.check_and_remove(
                (check, &mut check_paths),
                (root_path, &path, &mut dest_context),
                (this, parent),
            )?;
            Ok(())
        })?;
        Ok(())
    }

    fn walk_loop(
        &self,
        (num_files, total_size): (&mut u64, &mut u64),
        (this, parent, path, stack): (&Path, &Path, &Path, &mut Vec<PathBuf>),
        (check, check_path, check_paths): (bool, &Path, &mut Vec<PathBuf>),
        (dir, src_context): (DirectoryType, &mut NetworkContext),
    ) -> ForkliftResult<()> {
        for entrytype in dir {
            let entry = entrytype?;
            let file_path = entry.path();
            if file_path != this && file_path != parent {
                let newpath = path.join(&file_path);
                //file exists?
                let meta = self.process_file(&newpath, src_context, &self.nodes.clone())?;
                if let Some(meta) = meta {
                    *num_files += 1;
                    *total_size += meta.size() as u64;
                    match self.progress_output.send(ProgressMessage::Todo {
                        num_files: *num_files,
                        total_size: *total_size as usize,
                    }) {
                        Ok(_) => {}
                        Err(e) => {
                            return Err(ForkliftError::FSError(format!(
                                "Error: {:?}, unable to send progress",
                                e
                            )));
                        }
                    };
                }
                match entry.filetype() {
                    GenericFileType::Directory => {
                        debug!("dir: {:?}", &newpath);
                        stack.push(newpath.clone());
                    }
                    GenericFileType::File => {
                        debug!("file: {:?}", newpath);
                    }
                    GenericFileType::Link => {
                        debug!("link: {:?}", newpath);
                    }
                    GenericFileType::Other => {}
                }
                if check {
                    let check_path = check_path.join(file_path);
                    check_paths.push(check_path);
                }
            }
        }
        Ok(())
    }

    pub fn s_walk(
        &self,
        root_path: &Path,
        src_context: &mut NetworkContext,
        dest_context: &mut NetworkContext,
    ) -> ForkliftResult<()> {
        let (mut num_files, mut total_size) = (0, 0);
        let mut stack: Vec<PathBuf> = vec![self.source.clone()];
        let (this, parent) = (Path::new("."), Path::new(".."));
        loop {
            let check: bool;
            let mut check_paths: Vec<PathBuf> = vec![];
            match stack.pop() {
                Some(p) => {
                    let check_path = self.get_check_path(&p, root_path)?;
                    check = exist(&check_path, dest_context);
                    let dir = src_context.opendir(&p)?;
                    self.walk_loop(
                        (&mut num_files, &mut total_size),
                        (this, parent, &p, &mut stack),
                        (check, &check_path, &mut check_paths),
                        (dir, src_context),
                    )?;
                    // check through dest files
                    self.check_and_remove(
                        (check, &mut check_paths),
                        (root_path, &p, dest_context),
                        (this, parent),
                    )?;
                }
                None => {
                    debug!("Total number of files sent {:?}", num_files);
                    break;
                }
            }
        }
        Ok(())
    }

    fn get_check_path(&self, source_path: &Path, root_path: &Path) -> ForkliftResult<PathBuf> {
        let rel_path = get_rel_path(&source_path, &self.source)?;
        Ok(root_path.join(rel_path))
    }

    fn check_and_remove(
        &self,
        (check, check_paths): (bool, &mut Vec<PathBuf>),
        (root_path, source_path, dest_context): (&Path, &Path, &mut NetworkContext),
        (this, parent): (&Path, &Path),
    ) -> ForkliftResult<()> {
        // check through dest files
        if check {
            let check_path = self.get_check_path(&source_path, root_path)?;
            let dir = dest_context.opendir(&check_path)?;
            for entrytype in dir {
                let entry = entrytype?;
                let file_path = entry.path();
                if file_path != this && file_path != parent {
                    let newpath = check_path.join(file_path);
                    if !contains_and_remove(check_paths, &newpath) {
                        match entry.filetype() {
                            GenericFileType::Directory => {
                                trace!("call remove_dir: {:?}", &newpath);
                                remove_dir(&newpath, dest_context)?;
                            }
                            _ => {
                                debug!("remove: {:?}", &newpath);
                                remove_extra(&newpath, dest_context)?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn process_file(
        &self,
        entry: &Path,
        src_context: &mut NetworkContext,
        nodes: &Arc<Mutex<RendezvousNodes<SocketNode, DefaultNodeHasher>>>,
    ) -> ForkliftResult<Option<Stat>> {
        let n = match nodes.lock() {
            Ok(e) => {
                let mut list = e;
                info!(
                    "{:?}",
                    list.calc_candidates(&entry.to_string_lossy())
                        .collect::<Vec<_>>()
                );
                match list.calc_candidates(&entry.to_string_lossy()).nth(0) {
                    Some(p) => p.clone(),
                    None => {
                        return Err(ForkliftError::FSError("calc candidates failed".to_string()));
                    }
                }
            }
            Err(_) => {
                error!("Failed to lock!");
                return Err(ForkliftError::FSError("failed to lock".to_string()));
            }
        };
        if n == self.node {
            let src_entry = Entry::new(entry, src_context);
            let metadata = match src_entry.metadata() {
                Some(stat) => stat,
                None => {
                    return Ok(None);
                }
            };
            //Note, send only returns an error should the channel disconnect ->
            //Should we attempt to reconnect the channel?
            self.do_work(Some(src_entry.clone()))?;
            return Ok(Some(metadata));
        }
        Ok(None)
    }
}

fn contains_and_remove(check_paths: &mut Vec<PathBuf>, check_path: &Path) -> bool {
    for (count, source_path) in check_paths.iter().enumerate() {
        if source_path == check_path {
            check_paths.remove(count);
            return true;
        }
    }
    false
}

fn remove_extra(path: &Path, dest_context: &mut NetworkContext) -> ForkliftResult<()> {
    dest_context.unlink(path)
}

fn remove_dir(path: &Path, dest_context: &mut NetworkContext) -> ForkliftResult<()> {
    let (this, parent) = (Path::new("."), Path::new(".."));
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
            if file_path != this && file_path != parent {
                let newpath = p.join(&file_path);
                debug!("remove: {:?}", &newpath);
                match entry.filetype() {
                    GenericFileType::Directory => {
                        stack.push(newpath.clone());
                        remove_stack.push(newpath);
                    }
                    GenericFileType::File => {
                        remove_extra(&newpath, dest_context)?;
                    }
                    GenericFileType::Link => {
                        remove_extra(&newpath, dest_context)?;
                    }
                    GenericFileType::Other => {}
                }
            }
        }
    }
    while !remove_stack.is_empty() {
        let dir = match remove_stack.pop() {
            Some(e) => e,
            None => {
                return Err(ForkliftError::FSError(
                    "remove stack should not be empty!".to_string(),
                ));
            }
        };
        dest_context.rmdir(&dir)?;
    }
    Ok(())
}
