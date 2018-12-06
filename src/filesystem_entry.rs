extern crate digest;
extern crate meowhash;
extern crate nix;
extern crate smbc;

use self::digest::Digest;
use self::meowhash::*;
use self::nix::fcntl::OFlag;
use self::nix::sys::stat::{Mode, SFlag};
use error::{ForkliftError, ForkliftResult};
use filesystem::*;
use smbc::*;
use std::path::{Path, PathBuf};

const BUFF_SIZE: u64 = 1024 * 1000;

#[derive(Clone)]
pub struct Entry {
    path: PathBuf,
    metadata: Option<Stat>,
    is_link: Option<bool>,
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
            Some(m) => Some(m.mode() & SFlag::S_IFMT.bits() == SFlag::S_IFLNK.bits()),
            None => None,
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

    pub fn is_link(&self) -> Option<bool> {
        self.is_link
    }
}

#[derive(PartialEq, Debug)]
pub enum SyncOutcome {
    UpToDate,
    FileCopied,
    FileUpdated,
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
/// NOTE: this only checks mtime, since ctime are attr changes and we want
/// to know if there were any recent write changes
pub fn is_more_recent(src: &Entry, dest: &Entry) -> bool {
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

    src_mtime.num_microseconds() > dest_mtime.num_microseconds()
}

pub fn has_different_permissions(
    src: &Entry,
    src_context: &NetworkContext,
    dest: &Entry,
    dest_context: &NetworkContext,
) -> bool {
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
        NetworkContext::Nfs(_) => {
            match dest_context {
                //check for matching mode values from stat
                NetworkContext::Nfs(_) => {
                    if src_meta.mode() != dest_meta.mode() {
                        return true;
                    } else {
                        return false;
                    }
                }
                NetworkContext::Samba(_) => {
                    error!("Filesystems do not match!");
                    panic!("Filesystems do not match!")
                }
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
                    let src_acl_values = ctx
                        .getxattr(
                            src.path().as_path(),
                            &SmbcXAttr::AclAttr(SmbcAclAttr::AclAll),
                        ).unwrap();
                    let dest_acl_values = dctx
                        .getxattr(
                            dest.path().as_path(),
                            &SmbcXAttr::AclAttr(SmbcAclAttr::AclAll),
                        ).unwrap();

                    let src_mod_values = ctx
                        .getxattr(src.path().as_path(), &SmbcXAttr::DosAttr(SmbcDosAttr::Mode))
                        .unwrap();
                    let dest_mod_values = dctx
                        .getxattr(
                            dest.path().as_path(),
                            &SmbcXAttr::DosAttr(SmbcDosAttr::Mode),
                        ).unwrap();
                    src_acl_values != dest_acl_values || src_mod_values != dest_mod_values
                }
            }
        }
    }
}

fn make_target(size: i64, readmax: u64) -> ForkliftResult<Vec<u8>> {
    let mut src_target: Vec<u8>;
    if size <= readmax as i64 {
        if size > 0 {
            src_target = Vec::with_capacity(size as usize);
            unsafe {
                src_target.set_len(size as usize);
            }
        } else {
            src_target = Vec::with_capacity(readmax as usize);
            unsafe {
                src_target.set_len(readmax as usize);
            }
        }
    } else {
        return Err(ForkliftError::FSError("File Name too long".to_string()));
    }
    Ok(src_target)
}

pub fn copy_link(
    src: &Entry,
    src_context: &NetworkContext,
    dest: &Entry,
    dest_context: &NetworkContext,
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
    let src_path = src.path();
    let readmax = context.get_readmax()?;
    let mut src_target: Vec<u8> = make_target(src_size, readmax)?;

    match context.readlink(src_path.as_path(), &mut src_target) {
        Ok(_) => (),
        Err(e) => {
            let err = format!(
                "Unable to read link at {}, {:?}",
                src_path.to_string_lossy(),
                e
            );
            return Err(ForkliftError::FSError(err));
        }
    };
    let src_target = String::from_utf8(src_target)?;
    let outcome: SyncOutcome;
    let dest_path = dest.path();
    match dest.is_link() {
        Some(true) => {
            //NOTE, this is safe since is_link is SOME(true) only if metadata exists
            let dest_stat = dest.metadata().unwrap();
            //read the link target into buf
            let dest_size = dest_stat.size();
            let dest_path = dest.path();
            let readmax = dcontext.get_readmax()?;
            let mut dest_target: Vec<u8> = make_target(dest_size, readmax)?;
            match dcontext.readlink(dest_path.as_path(), &mut dest_target) {
                Ok(_) => (),
                Err(e) => {
                    let err = format!(
                        "Unable to read link at {}, {:?}",
                        src_path.to_string_lossy(),
                        e
                    );
                    return Err(ForkliftError::FSError(err));
                }
            };
            let dest_target = String::from_utf8(dest_target)?;
            if dest_target != src_target {
                match dcontext.unlink(dest_path.as_path()) {
                    Ok(_) => (),
                    Err(e) => {
                        let err = format!(
                            "Could not remove {:?} while updating link, {}",
                            dest_path, e
                        );
                        return Err(ForkliftError::FSError(err));
                    }
                }
            }
            outcome = SyncOutcome::SymlinkUpdated
        }
        Some(false) => {
            //Not safe to delete...
            let err = format!(
                "Refusing to replace existing path {:?} by symlink",
                dest_path
            );
            return Err(ForkliftError::FSError(err));
        }
        None => {
            outcome = SyncOutcome::SymlinkCreated;
        }
    }

    match dcontext.symlink(Path::new(&src_target), dest.path().as_path()) {
        Ok(_) => (),
        Err(e) => {
            let err = format!(
                "Error {}, Could not create link from {} to {:?}",
                e,
                dest_path.to_string_lossy(),
                src_target
            );
            return Err(ForkliftError::FSError(err));
        }
    };
    Ok(outcome)
}

fn read_chunk(file: &FileType, offset: u64, path: &PathBuf) -> ForkliftResult<Vec<u8>> {
    let buffer = match file.read(BUFF_SIZE, offset) {
        Ok(buf) => buf,
        Err(e) => {
            let err = format!(
                "Error {:?}, Could not read from {}",
                e,
                path.to_string_lossy(),
            );
            return Err(ForkliftError::FSError(err));
        }
    };
    if buffer.len() <= 0 {
        return Ok(buffer);
    }
    Ok(buffer)
}

fn open_file(
    context: &mut NetworkContext,
    path: &PathBuf,
    flags: OFlag,
    error: &str,
) -> ForkliftResult<FileType> {
    match context.open(path.as_path(), flags, Mode::S_IRWXU) {
        Ok(f) => Ok(f),
        Err(e) => {
            let err = format!("Error {}, {}", e, error);
            return Err(ForkliftError::FSError(err));
        }
    }
}

pub fn copy_entry(
    src: &Entry,
    src_context: &mut NetworkContext,
    dest: &Entry,
    dest_context: &mut NetworkContext,
) -> ForkliftResult<SyncOutcome> {
    //check if src exists (which it should...)
    if !exist(src.path(), src_context) {
        panic!("Source file should exist!");
    }
    let err = format!(
        "Could not open {} for reading",
        src.path().to_string_lossy()
    );
    let src_file = open_file(src_context, src.path(), OFlag::O_RDONLY, &err)?;

    let dest_file = match dest_context.create(
        &dest.path().as_path(),
        OFlag::O_RDWR | OFlag::O_CREAT,
        Mode::S_IRWXU,
    ) {
        Ok(f) => f,
        Err(e) => {
            println!("Error {:?}", e);
            let err = format!(
                "Could not open {} for writing",
                dest.path().to_string_lossy()
            );
            return Err(ForkliftError::FSError(err));
        }
    };

    //let mut buffer = vec![0, BUFF_SIZE];
    let mut offset = 0;
    let mut end = false;
    while { !end } {
        let buffer = read_chunk(&src_file, offset, src.path())?;
        let num_written = match dest_file.write(&buffer, offset) {
            Ok(n) => n,
            Err(e) => {
                let err = format!(
                    "Error {}, Could not write to {}",
                    e,
                    dest.path().to_string_lossy()
                );
                return Err(ForkliftError::FSError(err));
            }
        };
        if num_written == 0 {
            end = true;
        }
        offset = offset + num_written as u64;
        //INSERT PROGRESS MESSAGE HERE
        //SEND PROGRESS
    }
    Ok(SyncOutcome::FileCopied)
}

pub fn checksum_copy(
    src: &Entry,
    src_context: &mut NetworkContext,
    dest: &Entry,
    dest_context: &mut NetworkContext,
) -> ForkliftResult<SyncOutcome> {
    //check if src exists (which it should...)
    if !exist(src.path(), src_context) {
        panic!("Source file should exist!");
    }
    // open src file
    let err = format!(
        "Could not open {} for reading",
        src.path().to_string_lossy()
    );
    let src_file = open_file(src_context, src.path(), OFlag::O_RDONLY, &err)?;
    //open dest file
    let dest_file = open_file(
        dest_context,
        dest.path(),
        OFlag::O_RDWR | OFlag::O_CREAT,
        &err,
    )?;

    //loop until end
    let mut offset = 0;
    let mut meowhash = MeowHasher::new();
    let mut end = false;
    let mut counter = 0; //count number of times we needed to update the file
    while { !end } {
        let mut num_written: i32 = 0;
        //read 1M from src
        let src_buf = read_chunk(&src_file, offset, src.path())?;
        //hash src_buf
        meowhash.input(&src_buf);
        let hash_src = meowhash.result_reset();
        //read 1M from dest
        let dest_buf = read_chunk(&dest_file, offset, dest.path())?;
        //hash dest_buf
        meowhash.input(&dest_buf);
        let hash_dest = meowhash.result_reset();
        //if hash_src != has_dest
        if hash_src != hash_dest {
            if src_buf.len() < dest_buf.len() {
                dest_file.truncate(src_buf.len() as u64)?;
            }

            //write src_buf -> dest
            num_written = match dest_file.write(&src_buf, offset) {
                Ok(n) => n,
                Err(e) => {
                    let err = format!(
                        "Error {}, Could not write to {}",
                        e,
                        dest.path().to_string_lossy()
                    );
                    return Err(ForkliftError::FSError(err));
                }
            };
            //update counter
            counter += 1;
        }
        //update offset
        //check if num_written > 0
        if num_written > 0 {
            offset = 0;
        } else {
            offset += src_buf.len() as u64;
        }
        if src_buf.len() == 0 {
            end = true;
        }
    } //end loop

    if counter == 0 {
        return Ok(SyncOutcome::UpToDate);
    }
    Ok(SyncOutcome::FileUpdated)
}
