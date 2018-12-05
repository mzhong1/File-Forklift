extern crate nix;
extern crate smbc;

use self::nix::sys::stat::{Mode, SFlag};
use error::{ForkliftError, ForkliftResult};
use filesystem::*;
use smbc::*;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct Entry {
    path: PathBuf,
    metadata: Option<Stat>,
    is_link: bool,
}

impl Entry {
    pub fn new(epath: &Path, context: &NetworkContext) -> Self {
        let metadata = match context.stat(epath) {
            Ok(stat) => Some(stat),
            Err(e) => {
                error!("File {} does not exist! {}", epath.to_string_lossy(), e);
                println!("Error {:?}", e);
                None // note: file DNE
            }
        };
        let is_link = match metadata {
            Some(m) => m.mode() & SFlag::S_IFMT.bits() == SFlag::S_IFLNK.bits(),
            None => false,
        };
        Entry {
            path: epath.to_path_buf(),
            metadata,
            is_link,
        }
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn metadata(&self) -> Option<Stat> {
        self.metadata
    }

    pub fn is_link(&self) -> bool {
        self.is_link
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
pub fn exist(path: &Path, fs: &mut NetworkContext) -> bool {
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
pub fn make_dir_all(p: &Path, fs: &mut NetworkContext) {
    let mut stack = vec![];
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

/// Since size attributes remain the same for samba + nfs calls,
/// Can do comparison
/// returns true if src, dest files size are different or dest file does not exist
/// false otherwise
pub fn has_different_size(src: &Entry, dest: &Entry) -> bool {
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
        None => true, //Dest file does not exist
    }
}

/// Since #time attributes remain the same for samba + nfs calls,
/// we can do comparison.
/// returns true if src is more recent than dest (need to update dest then...)
fn is_more_recent(src: &Entry, dest: &Entry) -> bool {
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

    let src_mtime = src_meta.mtime();
    let dest_mtime = dest_meta.mtime();

    if src_mtime.num_microseconds() > dest_mtime.num_microseconds() {
        return true;
    }

    //check c_time for inode changes (owner, group, link, mode, etc.)
    let src_ctime = src_meta.ctime();
    let dest_ctime = dest_meta.ctime();
    src_ctime.num_microseconds() > dest_ctime.num_microseconds()
}

fn has_different_permissions(
    src: &mut Entry,
    src_context: &mut NetworkContext,
    dest: &mut Entry,
    dest_context: &mut NetworkContext,
) -> bool {
    //check if context types are the same (which they really should be...)
    let matching_context = match src_context {
        NetworkContext::Nfs(_) => match dest_context {
            NetworkContext::Nfs(_) => true,
            NetworkContext::Samba(_) => false,
        },
        NetworkContext::Samba(ctx) => match dest_context {
            NetworkContext::Nfs(_) => false,
            NetworkContext::Samba(_) => true,
        },
    };
    if !matching_context {
        error!("Filesystems do not match!");
        panic!("Filesystems do not match!")
    }

    //check file existence
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

    match src_context {
        NetworkContext::Nfs(_) =>
        //check for matching mode values from stat
        {
            if src_meta.mode() != dest_meta.mode() {
                return true;
            } else {
                return false;
            }
        }
        NetworkContext::Samba(ctx) => {
            //We want to check that XAttr's match
            match dest_context {
                NetworkContext::Nfs(_) =>
                //This shouldn't happen since we already checked
                {
                    error!("Filesystems do not match!");
                    panic!("Filesystems do not match!")
                }
                NetworkContext::Samba(dctx) => {
                    let src_xattr_values =
                        ctx.getxattr(src.path().as_path(), &SmbcXAttr::All).unwrap();
                    let dest_xattr_values = dctx
                        .getxattr(dest.path().as_path(), &SmbcXAttr::All)
                        .unwrap();
                    src_xattr_values == dest_xattr_values
                }
            }
        }
    }
}

fn copy_link(
    src: &Entry,
    src_context: &mut NetworkContext,
    dest: &mut Entry,
    dest_context: &mut NetworkContext,
) -> ForkliftResult<SyncOutcome> {
    //Check if correct Filesytem
    let context = match src_context {
        NetworkContext::Samba(_) => {
            return Err(ForkliftError::FSError(
                "Samba does not support symlinks".to_string(),
            ));
        }
        NetworkContext::Nfs(ctx) => ctx,
    };
    let dcontext = match dest_context {
        NetworkContext::Samba(_) => {
            return Err(ForkliftError::FSError(
                "Samba does not support symlinks".to_string(),
            ));
        }
        NetworkContext::Nfs(ctx) => ctx,
    };
    //Check if files exist....
    let src_stat = match src.metadata() {
        Some(stat) => stat,
        None => {
            return Err(ForkliftError::FSError(
                "Source File does not exist!".to_string(),
            ));
        }
    };
    //read the link target into buf
    let src_size = src_stat.size();
    let readmax = context.get_readmax()?;
    let mut buf: Vec<u8> = vec![];
    if src_size <= readmax as i64 {
        if src_size > 0 {
            let mut buf: Vec<u8> = Vec::with_capacity(src_size as usize);
            unsafe {
                buf.set_len(src_size as usize);
            }
        } else {
            let mut buf: Vec<u8> = Vec::with_capacity(readmax as usize);
            unsafe {
                buf.set_len(readmax as usize);
            }
        }
    } else {
        return Err(ForkliftError::FSError("File Name too long".to_string()));
    }
    context.readlink(src.path().as_path(), &mut buf)?;

    let outcome: SyncOutcome;
    if dest.is_link() {
        let mut dbuf;
        match dest.metadata() {
            Some(stat) => {
                let dest_size = stat.size();
                let readmax = dcontext.get_readmax()?;
                if dest_size <= readmax as i64 {
                    if src_size > 0 {
                        dbuf = Vec::with_capacity(src_size as usize);
                        unsafe {
                            buf.set_len(src_size as usize);
                        }
                    } else {
                        dbuf = Vec::with_capacity(readmax as usize);
                        unsafe {
                            buf.set_len(readmax as usize);
                        }
                    }
                } else {
                    return Err(ForkliftError::FSError("File Name too long".to_string()));
                }
                context.readlink(dest.path().as_path(), &mut dbuf)?;
                outcome = SyncOutcome::SymlinkUpdated;
            }
            None => {
                outcome = SyncOutcome::SymlinkCreated;
            }
        }
    } else {
        let err = format!(
            "Refusing to replace existing path {:?} by symlink",
            dest.path()
        );
        return Err(ForkliftError::FSError(err));
    }

    dcontext.symlink(Path::new(&String::from_utf8(buf)?), dest.path().as_path())?;
    Ok(outcome)
}
