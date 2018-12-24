use crate::error::{ForkliftError, ForkliftResult};
use crate::filesystem::*;
use crate::filesystem_entry::Entry;
use crate::filesystem_ops::*;
use crate::progress_message::ProgressMessage;

use ::libnfs::*;
use ::smbc::*;
use crossbeam::*;

use std::path::{Path, PathBuf};

pub struct WalkWorker {
    entry_output: Sender<Entry>,
    progress_output: Sender<ProgressMessage>,
    source: PathBuf,
}

impl WalkWorker {
    pub fn new(
        source: &Path,
        entry_output: Sender<Entry>,
        progress_output: Sender<ProgressMessage>,
    ) -> WalkWorker {
        WalkWorker {
            entry_output,
            progress_output,
            source: source.to_path_buf(),
        }
    }

    pub fn walk(&self, src_context: &mut NetworkContext) -> ForkliftResult<()> {
        let (mut num_files, mut total_size) = (0, 0);
        let mut done = false;
        let mut stack: Vec<PathBuf> = vec![self.source.clone()];
        let (this, parent) = (Path::new("."), Path::new(".."));
        while !done {
            match stack.pop() {
                Some(p) => match src_context {
                    NetworkContext::Nfs(nfs) => {
                        let dir = nfs.opendir(&p)?;
                        for f in dir {
                            let file = match f {
                                Ok(f) => f,
                                Err(e) => {
                                    return Err(ForkliftError::IoError(e));
                                }
                            };
                            if file.path != this && file.path != parent {
                                let newpath = p.join(file.path);
                                match file.d_type {
                                    EntryType::Directory => {
                                        println!("dir: {:?}", &newpath);
                                        stack.push(newpath.clone());
                                    }
                                    EntryType::File => {
                                        println!("file: {:?}", newpath);
                                    }
                                    EntryType::Symlink => {
                                        println!("link: {:?}", newpath);
                                    }
                                    _ => {}
                                }
                                let meta = self.process_file(&newpath, src_context);
                                if let Some(meta) = meta {
                                    num_files += 1;
                                    total_size += meta.size();
                                    self.progress_output.send(ProgressMessage::Todo {
                                        num_files,
                                        total_size: total_size as usize,
                                    });
                                }
                            }
                        }
                    }
                    NetworkContext::Samba(smb) => {
                        let dir = smb.opendir(&p)?;
                        for f in dir {
                            let file = match f {
                                Ok(f) => f,
                                Err(e) => {
                                    return Err(ForkliftError::IoError(e));
                                }
                            };
                            if file.path != this && file.path != parent {
                                let newpath = p.join(file.path);
                                match file.s_type {
                                    SmbcType::DIR => {
                                        println!("dir: {:?}", &newpath);
                                        stack.push(newpath.clone());
                                    }
                                    SmbcType::FILE => {
                                        println!("file: {:?}", &newpath);
                                    }
                                    SmbcType::LINK => {
                                        println!("link: {:?}", &newpath);
                                    }
                                    _ => {}
                                }
                                let meta = self.process_file(&newpath, src_context);
                                if let Some(meta) = meta {
                                    num_files += 1;
                                    total_size += meta.size();
                                    self.progress_output.send(ProgressMessage::Todo {
                                        num_files,
                                        total_size: total_size as usize,
                                    });
                                }
                            }
                        }
                    }
                },
                None => {
                    done = true;
                }
            }
        }
        Ok(())
    }

    fn process_file(&self, entry: &Path, src_context: &mut NetworkContext) -> Option<Stat> {
        let src_entry = Entry::new(entry, src_context);
        let metadata = match src_entry.metadata() {
            Some(stat) => stat,
            None => {
                return None;
            }
        };

        self.entry_output.send(src_entry.clone());

        Some(metadata.clone())
    }
}
