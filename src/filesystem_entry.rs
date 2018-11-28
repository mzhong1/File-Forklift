extern crate nix;

use self::nix::sys::stat::{Mode, SFlag};
use error::{ForkliftError, ForkliftResult};
use filesystem::*;
use std::path::{Path, PathBuf};

#[derive(Clone)]
pub struct Entry {
    context: NetworkContext,
    path: PathBuf,
    metadata: Option<Stat>,
    is_link: bool,
}

impl Entry {
    pub fn new(epath: &Path, context: &NetworkContext) -> Self {
        let metadata = match context.stat(epath) {
            Ok(stat) => Some(stat),
            Err(e) => {
                error!("stat failed! {}", e);
                None // note: file DNE
            }
        };
        let is_link = match metadata {
            Some(m) => m.mode() & SFlag::S_IFMT & SFlag::S_IFLNK != 0,
            None => false,
        };
        Entry {
            context,
            path: epath.to_path_buf(),
            metadata,
            is_link,
        }
    }

    pub fn context(&self) -> NetworkContext {
        &self.context
    }

    pub fn path(&self) -> PathBuf {
        &self.path
    }

    pub fn metadata(&self) -> Option<Stat> {
        &self.metadata
    }

    pub fn is_link(&self) -> bool {
        &self.is_link
    }
}

#[derive(PartialEq, Debug)]
pub enum SyncOutcome {
    UpToDate,
    FileCopied,
    SymlinkUpdated,
    SymlinkCreated,
}

//stick in filesystemtype?
pub fn exist<F: FileSystem>(path: &Path, fs: F) -> bool {
    match fs.stat(path) {
        Ok(_) => true,
        Err(_) => false,
    }
}

/***
 * NOTE: MOVE THIS FUNCTION ELSEWHERE
 * Also, it looks like the only errors that should occur
 * are the ones where mkdir fails
 */
pub fn make_dir_all<F: FileSystem>(p: &Path, fs: F) {
    let stack = vec![];
    let q = p.parent();
    while { q != None } {
        stack.push(q.unwrap()); //Note, can do b/c loop invariant (q must be Some(t))
        let q = p.parent();
    }

    while !stack.is_empty() {
        let path = stack.pop().unwrap(); //Poss b/c loop invariant (stack not empty)

        if !exist(&path, fs) {
            fs.mkdir(path);
        }
    }
}

/***
 * NOTE: MOVE THIS FUNCTION ELSEWHERE
 */
/*pub fn copy_entry<F: FileSystem>(sf: F, ds: F) {
    let src_path = sf.path();
    let mut src_file = sf.open(src_path).unwrap(); //match this with error
                                                   //let src_meta = sf.stat
}*/

fn has_different_size(src: &Entry, dest: &Entry) -> bool {
    let src_meta = match src.metadata() {
        Some(stat) => stat,
        None => {
            error!("Should not be None!");
            panic!("Should not be None!")
        }
    };
    let src_size = src_meta.size();

    match dest.metadata() {
        Some(stat) => stat.size() != src_size,
        None => true,
    }
}

fn is_more_recent(src: &Entry, dest: &Entry) -> bool {
    let dest_meta = match dest.metadata {
        Some(stat) => stat,
        None => {
            trace!("Dest File does not exist");
            return true;
        }
    };

    let src_meta = match src.metadata() {
        Some(stat) => stat,
        None => {
            error!("Source File does not Exist");
            panic!("Source File does not exist")
        }
    };

    let src_mtime = src_meta.mtime();
    let dest_mtime = dest_meta.mtime();

    if src_mtime.num_microseconds() > dest_mtime.num_microseconds() {
        return true;
    }

    let src_ctime = src_meta.ctime();
    let dest_ctime = dest_meta.ctime();
    src_ctime.num_microseconds() > dest_ctime.num_microseconds()
}

fn has_different_permissions(src: &Entry, dest: &Entry) -> bool {
    let dest_meta = match dest.metadata() {
        Some(stat) => stat,
        None => {
            trace!("Dest File does not exist");
            return true;
        }
    };

    let src_meta = match src.metadata() {
        Some(stat) => stat,
        None => {
            error!("Source File does not Exist");
            panic!("Source File does not exist")
        }
    };

    if src_meta.mode() != dest_meta.mode() {
        return true;
    }

    match src.context() {
        NetworkContext::Nfs(_) => false,
        NetworkContext::Samba(ctx) => {
            //check xattr differences
            true
        }
    }
}

//NOTE:
fn copy_link(src: &Entry, dest: &Entry) -> ForkliftResult<SyncOutcome> {
    //Check if correct Filesytem
    let context = match src.context() {
        NetworkContext::Samba(_) => {
            return ForkliftError::FSError("Samba does not support symlinks");
        }
        NetworkContext::Nfs(ctx) => ctx,
    };
    let dcontext = match dest.context() {
        NetworkContext::Samba(_) => {
            return ForkliftResult::FSError("Samba does not support symlinks");
        }
        NetworkContext::Nfs(ctx) => ctx,
    };
    //Check if files exist....
    let src_stat = match src.metadata() {
        Some(stat) => stat,
        None => {
            return ForkliftResult::FSError("Source File does not exist!");
        }
    };
    //this one is okay....
    let dest_stat = match dest.metadata() {
        Some(stat) => stat,
        None => {
            return ForkliftResult::FSError("Source File does not exist!");
        }
    };
    //read the link target into buf
    let src_size = src_stat.size();
    let readmax = context.get_readmax()?;
    let mut buf: Vec<u8> = vec![];
    if src_size <= readmax {
        if src_size > 0 {
            let mut buf: Vec<u8> = Vec::with_capacity(src_size);
            buf.set_len(src_size);
        } else {
            let mut buf: Vec<u8> = Vec::with_capacity(readmax);
            buf.set_len(readmax);
        }
    } else {
        return ForkliftResult::FSError("File Name too long");
    }
    context.readlink(src.path().as_path(), &buf)?;

    let outcome;
    let dest_size = dest_stat.size();
    let readmax = dcontext.get_readmax()?;
    if dest.is_link() {
        let dbuf = vec![];
        if dest_size <= readmax {
            if dest_size > 0 {
                let dbuf: Vec<u8> = Vec::with_capacity(dest_size);
                dbuf.set_len(dest_size);
            } else {
                let dbuf: Vec<u8> = Vec::with_capacity(readmax);
                dbuf.set_len(readmax);
            }
            dcontext.readlink(dest.path.as_path(), &dbuf)?;
        } else {
            return ForkliftResult::FSError("File Name too long");
        }
        if dbuf != buf {
            dcontext.unlink(dest.path())?;
            outcome = SyncOutcome::SymlinkUpdated;
        } else {
            return Ok(SyncOutcome::UpToDate);
        }
    } else {
        return ForkliftResult::FSError(
            "Refusing to replace existing path {:?} by symlink",
            dest.path(),
        );
    }

    dcontext.symlink(String::from_utf8(buf), dest.path().as_path())?;
    Ok(SyncOutcome::SymlinkUpdated)
}
