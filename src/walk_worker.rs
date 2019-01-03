use crate::error::{ForkliftError, ForkliftResult};
use crate::filesystem::*;
use crate::filesystem_entry::Entry;
use crate::filesystem_ops::*;
use crate::progress_message::ProgressMessage;

use ::libnfs::*;
use ::smbc::*;
use crossbeam::*;
use rayon::*;

use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

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

    pub fn t_walk(
        &self,
        root_path: &Path,
        path: &Path,
        src_context: &mut NetworkContext,
        dest_context: &mut NetworkContext,
    ) -> ForkliftResult<()> {
        rayon::scope(|spawner| {
            let (mut num_files, mut total_size) = (0, 0);
            let (this, parent) = (Path::new("."), Path::new(".."));
            let check: bool;
            let mut check_paths: Vec<PathBuf> = vec![];
            let check_path = self.get_check_path(&path, root_path)?;
            check = exist(&check_path, dest_context);
            match src_context {
                NetworkContext::Nfs(nfs) => {
                    let dir = nfs.opendir(&path)?;

                    for f in dir {
                        let file = match f {
                            Ok(f) => f,
                            Err(e) => {
                                error!("Error, non-unicode character in file path");
                                return Err(ForkliftError::IoError(e));
                            }
                        };
                        if file.path != this && file.path != parent {
                            let p = file.path;
                            let newpath = path.join(&p);
                            let meta = self.process_file(&newpath, src_context);
                            if let Some(meta) = meta {
                                num_files += 1;
                                total_size += meta.size();
                                self.progress_output.send(ProgressMessage::Todo {
                                    num_files,
                                    total_size: total_size as usize,
                                });
                            }

                            match file.d_type {
                                EntryType::Directory => {
                                    println!("dir: {:?}", &newpath);
                                    let rec_ctx = src_context.clone();
                                    let drec_ctx = dest_context.clone();
                                    spawner.spawn(|_| {
                                        let mut rec_ctx = rec_ctx;
                                        let mut drec_ctx = drec_ctx;
                                        let newpath = newpath;
                                        self.t_walk(
                                            &root_path,
                                            &newpath,
                                            &mut rec_ctx,
                                            &mut drec_ctx,
                                        )
                                        .unwrap()
                                    });
                                }
                                EntryType::File => {
                                    println!("file: {:?}", &newpath);
                                }
                                EntryType::Symlink => {
                                    println!("file: {:?}", &newpath);
                                }
                                _ => {}
                            }
                            if check {
                                let check_path = check_path.join(&p);
                                println!("check path {:?}", &check_path);
                                check_paths.push(check_path);
                            }
                        }
                    }
                }
                NetworkContext::Samba(smb) => {
                    let dir = smb.opendir(&path)?;
                    for f in dir {
                        let file = match f {
                            Ok(f) => f,
                            Err(e) => {
                                error!("Error, non-unicode character in file path");
                                return Err(ForkliftError::IoError(e));
                            }
                        };
                        if file.path != this && file.path != parent {
                            let p = file.path;
                            let newpath = path.join(&p);
                            let meta = self.process_file(&newpath, src_context);
                            if let Some(meta) = meta {
                                num_files += 1;
                                total_size += meta.size();
                                self.progress_output.send(ProgressMessage::Todo {
                                    num_files,
                                    total_size: total_size as usize,
                                });
                            }
                            if check {
                                let check_path = check_path.join(&p);
                                check_paths.push(check_path);
                            }
                            match file.s_type {
                                SmbcType::DIR => {
                                    println!("dir: {:?}", &newpath);
                                    let rec_ctx = src_context.clone();
                                    let drec_ctx = dest_context.clone();
                                    spawner.spawn(|_| {
                                        let mut rec_ctx = rec_ctx;
                                        let root_path = root_path;
                                        let mut drec_ctx = drec_ctx;
                                        let newpath = newpath;
                                        self.t_walk(
                                            &root_path,
                                            &newpath,
                                            &mut rec_ctx,
                                            &mut drec_ctx,
                                        )
                                        .unwrap()
                                    });
                                }
                                SmbcType::FILE => {
                                    println!("file: {:?}", &newpath);
                                }
                                SmbcType::LINK => {
                                    println!("file: {:?}", &newpath);
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            // check through dest files
            if check {
                let (this, parent) = (Path::new("."), Path::new(".."));
                let rel_path = get_rel_path(&path, &self.source)?;
                let check_path = root_path.join(rel_path);
                println!("check path {:?}", check_path);
                match dest_context {
                    NetworkContext::Nfs(nfs) => {
                        let dir = nfs.opendir(&check_path)?;
                        for f in dir {
                            let file = f?;
                            if file.path != this && file.path != parent {
                                let newpath = check_path.join(file.path);
                                //check if newpath in check_path
                                if !check_paths.contains(&newpath) {
                                    //remove the file
                                    println!("remove: {:?}", &newpath);
                                    self.remove_extra(&newpath, dest_context)?;
                                }
                            }
                        }
                    }
                    NetworkContext::Samba(smb) => {
                        let dir = smb.opendir(&check_path)?;
                        for f in dir {
                            let file = f?;
                            if file.path != this && file.path != parent {
                                let newpath = check_path.join(file.path);
                                //check if newpath in check_path
                                if !check_paths.contains(&newpath) {
                                    //remove the file
                                    println!("remove: {:?}", &newpath);
                                    self.remove_extra(&newpath, dest_context)?;
                                }
                            }
                        }
                    }
                }
            }
            Ok(())
        })?;

        Ok(())
    }

    fn walk_nfs(
        &self,
        (check, check_path, check_paths): (&bool, &Path, &mut Vec<PathBuf>),
        (path, nfs, src_context): (&Path, &mut Nfs, &mut NetworkContext),
        (num_files, total_size, stack): (&mut u64, &mut i64, &mut Vec<PathBuf>),
        (this, parent): (&Path, &Path),
    ) -> ForkliftResult<()> {
        let dir = nfs.opendir(&path)?;
        for f in dir {
            let file = f?;
            if file.path != this && file.path != parent {
                let newpath = path.join(&file.path);
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
                    *num_files += 1;
                    *total_size += meta.size();
                    self.progress_output.send(ProgressMessage::Todo {
                        num_files: *num_files,
                        total_size: *total_size as usize,
                    });
                }
                if *check {
                    let check_path = check_path.join(file.path);
                    check_paths.push(check_path);
                }
            }
        }
        Ok(())
    }

    fn walk_smb(
        &self,
        (check, check_path, check_paths): (&bool, &Path, &mut Vec<PathBuf>),
        (path, smb, src_context): (&Path, &mut Smbc, &mut NetworkContext),
        (num_files, total_size, stack): (&mut u64, &mut i64, &mut Vec<PathBuf>),
        (this, parent): (&Path, &Path),
    ) -> ForkliftResult<()> {
        let dir = smb.opendir(&path)?;
        for f in dir {
            let file = f?;
            if file.path != this && file.path != parent {
                let newpath = path.join(&file.path);
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
                    *num_files += 1;
                    *total_size += meta.size();
                    self.progress_output.send(ProgressMessage::Todo {
                        num_files: *num_files,
                        total_size: *total_size as usize,
                    });
                }
                if *check {
                    let check_path = check_path.join(file.path);
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
                    match &src_context {
                        NetworkContext::Nfs(nfs) => {
                            let mut rec_ctx = src_context.clone();
                            let mut send = nfs.clone();
                            self.walk_nfs(
                                (&check, &check_path, &mut check_paths),
                                (&p, &mut send, &mut rec_ctx),
                                (&mut num_files, &mut total_size, &mut stack),
                                (this, parent),
                            )?;
                        }
                        NetworkContext::Samba(smb) => {
                            let mut rec_ctx = src_context.clone();
                            let mut send = smb.clone();
                            self.walk_smb(
                                (&check, &check_path, &mut check_paths),
                                (&p, &mut send, &mut rec_ctx),
                                (&mut num_files, &mut total_size, &mut stack),
                                (this, parent),
                            )?;
                        }
                    }
                    // check through dest files
                    self.check_and_remove(
                        (check, &mut check_paths),
                        (root_path, &p, dest_context),
                        (this, parent),
                    )?;
                }
                None => {
                    break;
                }
            }
        }
        Ok(())
    }

    pub fn walk(&self, src_context: &mut NetworkContext) -> ForkliftResult<()> {
        let (mut num_files, mut total_size) = (0, 0);
        let mut stack: Vec<PathBuf> = vec![self.source.clone()];
        let (this, parent) = (Path::new("."), Path::new(".."));
        loop {
            match stack.pop() {
                Some(p) => match src_context {
                    NetworkContext::Nfs(nfs) => {
                        let dir = nfs.opendir(&p)?;
                        for f in dir {
                            let file = f?;
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
                            let file = f?;
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
                    break;
                }
            }
        }
        Ok(())
    }

    pub fn remove_walk(
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
                    match src_context {
                        NetworkContext::Nfs(nfs) => {
                            let dir = nfs.opendir(&p)?;
                            for f in dir {
                                let file = f?;
                                if file.path != this && file.path != parent {
                                    let newpath = p.join(&file.path);
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
                                    if check {
                                        let check_path = check_path.join(file.path);
                                        check_paths.push(check_path);
                                    }
                                }
                            }
                        }
                        NetworkContext::Samba(smb) => {
                            let dir = smb.opendir(&p)?;
                            for f in dir {
                                let file = f?;
                                if file.path != this && file.path != parent {
                                    let newpath = p.join(&file.path);
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
                                    if check {
                                        let check_path = check_path.join(file.path);
                                        check_paths.push(check_path);
                                    }
                                }
                            }
                        }
                    }
                    // check through dest files
                    self.check_and_remove(
                        (check, &mut check_paths),
                        (root_path, &p, dest_context),
                        (this, parent),
                    )?;
                }
                None => {
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
            println!("check path {:?}", check_path);
            println!("check_paths {:?}", check_paths);
            match dest_context {
                NetworkContext::Nfs(nfs) => {
                    let dir = nfs.opendir(&check_path)?;
                    for f in dir {
                        let file = f?;
                        if file.path != this && file.path != parent {
                            let newpath = check_path.join(file.path);
                            //check if newpath in check_path
                            if !contains_and_remove(check_paths, &newpath) {
                                println!("remove: {:?}", &newpath);
                                self.remove_extra(&newpath, dest_context)?;
                            }
                            /*if !check_paths.contains(&newpath) {
                                //remove the file
                                println!("remove: {:?}", &newpath);
                                self.remove_extra(&newpath, dest_context)?;
                            }*/
                        }
                    }
                }
                NetworkContext::Samba(smb) => {
                    let dir = smb.opendir(&check_path)?;
                    for f in dir {
                        let file = f?;
                        if file.path != this && file.path != parent {
                            let newpath = check_path.join(file.path);
                            //check if newpath in check_path
                            if !contains_and_remove(check_paths, &newpath) {
                                println!("remove: {:?}", &newpath);
                                self.remove_extra(&newpath, dest_context)?;
                            }
                            /*if !check_paths.contains(&newpath) {
                                //remove the file
                                println!("remove: {:?}", &newpath);
                                self.remove_extra(&newpath, dest_context)?;
                            }*/
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn remove_extra(&self, path: &Path, dest_context: &mut NetworkContext) -> ForkliftResult<()> {
        dest_context.unlink(path)
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

fn contains_and_remove(check_paths: &mut Vec<PathBuf>, check_path: &Path) -> bool {
    for (count, source_path) in check_paths.iter().enumerate() {
        if source_path == check_path {
            check_paths.remove(count);
            return true;
        }
    }
    false
}
