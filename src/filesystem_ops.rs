extern crate digest;
extern crate meowhash;
extern crate nix;
extern crate nom;
extern crate pathdiff;
extern crate smbc;

use self::digest::Digest;
use self::meowhash::*;
use self::nom::types::CompleteByteSlice;
use nix::fcntl::OFlag;
use nix::sys::stat::Mode;
use pathdiff::*;

use crate::error::{ForkliftError, ForkliftResult};
use crate::filesystem::*;
use crate::filesystem_entry::Entry;
use libnfs::*;
use smbc::*;
use std::collections::hash_map::Entry as E;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const BUFF_SIZE: u64 = 1024 * 1000;

lazy_static! {
    pub static ref NAME_MAP: Mutex<HashMap<String, Sid>> = Mutex::new(HashMap::new());
}

#[derive(PartialEq, Debug)]
pub enum SyncOutcome {
    UpToDate,
    FileCopied,
    SymlinkUpdated,
    SymlinkCreated,
    PermissionsUpdated,
    DirectoryCreated,
    DirectoryUpdated,
    ChecksumUpdated,
}

///
/// checks if a path is valid
///
/// @param path     The path to be checked
///
/// @param fs       The filesystem context the path is checked against
///
/// @return         true if the path exists (is valid), false otherwise
///
pub fn exist(path: &Path, fs: &mut NetworkContext) -> bool {
    match fs.stat(path) {
        Ok(_) => true,
        Err(_) => false,
    }
}

pub fn get_rel_path(a: &Path, b: &Path) -> ForkliftResult<PathBuf> {
    match pathdiff::diff_paths(&a, &b) {
        None => {
            let err = format!("Could not get relative path from {:?} to {:?}", &a, &b);
            Err(ForkliftError::FSError(err))
        }
        Some(path) => Ok(path),
    }
}

///
/// set the external attribute of a destination file on a Samba server
///
/// @param path     The path of the file whose external attributes are
///                 being set
///
/// @param fs       the Samba filesystem of the file
///
/// @param attr     The attribute being set.  valid descriptors can be
///                 found in Smbc.
///
/// @param val      The value to be set in the external attribute
///
/// @param error    The error description should the set fail
///
/// @param success  The string to be printed to debug logs upon success
///
/// @note           See Smbc.rs for notes on setxattr
///
pub fn set_xattr(
    path: &Path,
    fs: &Smbc,
    attr: &SmbcXAttr,
    val: &SmbcXAttrValue,
    error: &str,
    success: &str,
) -> ForkliftResult<()> {
    match fs.setxattr(path, attr, val, XAttrFlags::SMBC_XATTR_FLAG_CREATE) {
        Ok(_) => {
            debug!("{}", success);
            Ok(())
        }
        Err(e) => {
            let err = format!("Error {}, {}", e, error);
            error!("{}", err);
            Err(ForkliftError::FSError(err))
        }
    }
}

///
/// get the external attribute of a destination file on a Samba server
///
/// @param path     The path of the file whose external attributes are
///                 being set
///
/// @param fs       the Samba filesystem of the file
///
/// @param attr     The attribute to be retrieved. valid descriptors
///                 can be found in Smbc.
///
/// @param error    The error description should the get fail
///
/// @param success  The string to be printed to debug logs upon success
///
/// @note           See Smbc.rs for notes on getxattr.  Please note that
///                 you can in fact do an exclude for .* (all) operations
///
pub fn get_xattr(
    path: &Path,
    fs: &Smbc,
    attr: &SmbcXAttr,
    error: &str,
    success: &str,
) -> ForkliftResult<Vec<u8>> {
    match fs.getxattr(path, attr) {
        Ok(buf) => {
            debug!("{}", success);
            Ok(buf)
        }
        Err(e) => {
            let err = format!("Error {}, {}", e, error);
            error!("{}", err);
            Err(ForkliftError::FSError(err))
        }
    }
}

///
/// make and/or update a directory endpoint in the destination filesytem,
/// keeping Dos or Unix permissions (depending on if the context is Samba or NFS)
///
/// @param src_path     the path of the equivalent directory from the source filesystem
///
/// @param fs           the source filesystem
///
/// @param dest_path    the path of the directory being created
///
/// @param destfs       the destination filesystem
///
/// @return             returns the Sync outcome (or an error)
///
/// @note           If the filesystem context is Samba CIFS, then please note
///                 that the mode of the directory cannot go below 555 (see chmod
///                 notes in Smbc)
///
pub fn make_dir(
    src_path: &Path,
    fs: &mut NetworkContext,
    dest_path: &Path,
    destfs: &mut NetworkContext,
) -> ForkliftResult<SyncOutcome> {
    let outcome: SyncOutcome;
    let exists = exist(dest_path, destfs);
    if !exists {
        if let Err(e) = destfs.mkdir(dest_path) {
            let err = format!("Error {}, Could not create {:?}", e, dest_path);
            error!("{}", err);
            return Err(ForkliftError::FSError(err));
        }
    }
    let (src_entry, dest_entry) = (Entry::new(&src_path, fs), Entry::new(&dest_path, destfs));
    // make sure permissions match
    let out = match copy_permissions(&src_entry, &fs, &dest_entry, &destfs) {
        Ok(out) => {
            debug!("Copy permissions successful");
            out
        }
        Err(e) => {
            return Err(e);
        }
    };
    match (exists, out) {
        (false, _) => outcome = SyncOutcome::DirectoryCreated,
        (_, SyncOutcome::PermissionsUpdated) => outcome = SyncOutcome::DirectoryUpdated,
        (_, _) => outcome = SyncOutcome::UpToDate,
    }
    Ok(outcome)
}

///
/// find any directories in the path that do not exist in the
/// filesystem and add them
///
/// @param dest_path    the destination file path being checked
///
/// @param src_path     the src path equivalent used to ensure any created
///                     directories have the same permissions as the source
///                     filesystem
///
/// @param root         the root filepath, to ensure correctness while looping
///                     over the parent directories
///
/// @param fs           the source filesystem
///
/// @param destfs       the destination filesystem
///                     
pub fn make_dir_all(
    dest_path: &Path,
    src_path: &Path,
    root: &Path,
    fs: &mut NetworkContext,
    destfs: &mut NetworkContext,
) -> ForkliftResult<()> {
    let (mut stack, mut src_stack) = (vec![], vec![]);
    let (mut dest_parent, mut src_parent) = (dest_path.parent(), src_path.parent());
    trace!("dest parent {:?}", root);
    //add parent directory paths to stack
    while {
        match dest_parent {
            Some(parent) => parent != root,
            None => false,
        }
    } {
        dest_parent = match dest_parent {
            Some(parent) => {
                stack.push(parent);
                parent.parent()
            }
            None => {
                return Err(ForkliftError::FSError(
                    "While loop invariant failed".to_string(),
                ));
            }
        };
        src_parent = match src_parent {
            Some(src_parent) => {
                src_stack.push(src_parent);
                src_parent.parent()
            }
            None => {
                return Err(ForkliftError::FSError(
                    "src_path is smaller than dest".to_string(),
                ));
            }
        };
    }
    //check all directories in the path
    while !stack.is_empty() {
        trace!("stack not empty");
        let (path, srcpath) = match (stack.pop(), src_stack.pop()) {
            (Some(p), Some(sp)) => (p, sp),
            (_, _) => {
                return Err(ForkliftError::FSError("Loop invariant failed".to_string()));
            }
        };
        match make_dir(srcpath, fs, &path, destfs) {
            Ok(_) => debug!("made dir {:?}", path),
            Err(e) => {
                return Err(e);
            }
        };
    }
    Ok(())
}

///
/// Since size attributes remain the same for samba + nfs calls,
/// Can do comparison
///
/// @param src      The source entry to compare to
///
/// @param dest     The destination entry to compare the size of
///
/// @return         true if src, dest files size are different or dest file does not exist
///                 false otherwise
///
pub fn has_different_size(src: &Entry, dest: &Entry) -> ForkliftResult<bool> {
    match (src.metadata(), dest.metadata()) {
        (None, _) => {
            let err = format!("File {:?} stat should not be None!", src.path());
            error!("{}", err);
            Err(ForkliftError::FSError(err))
        }
        (_, None) => Ok(true),
        (Some(src_stat), Some(dest_stat)) => Ok(src_stat.size() != dest_stat.size()),
    }
}

///
/// Since #time attributes remain the same for samba + nfs calls,
/// we can do comparison.
///
/// @param src      The source entry to compare to
///
/// @param dest     The destination entry to compare the size of
///
/// @return         true if src is more recent than dest (need to update dest then...)
///
/// @note           this only checks mtime, since ctime are attr changes and we want
///                 to know if there were any recent write changes
///
pub fn is_more_recent(src: &Entry, dest: &Entry) -> ForkliftResult<bool> {
    match (src.metadata(), dest.metadata()) {
        (None, _) => {
            let err = format!("Source File {:?} does not exist", src.path());
            error!("{}", err);
            Err(ForkliftError::FSError(err))
        }
        (_, None) => {
            trace!("Dest File does not exist");
            Ok(true)
        }
        (Some(src_stat), Some(dest_stat)) => {
            Ok(src_stat.mtime().num_microseconds() > dest_stat.mtime().num_microseconds())
        }
    }
}

///
/// this functions checks whether or not the destination file has the same
/// permission settings as the source file.
///
/// @param src          Source file entry
///
/// @param src_context  the context of the source filesystem
///
/// @param dest         Dest file entry
///
/// @param dest_context the context of the destination filesystem
///
/// @return             true if the source and destination have DIFFERENT permissions
///                     false otherwise.  
///
/// @note               this function ONLY checks the mode attribute (DOS or LINUX)
///                     this functions DOES NOT check the external attributes (xattr)
///                     of a file.  
///
pub fn has_different_permissions(
    src: &Entry,
    src_context: &NetworkContext,
    dest: &Entry,
    dest_context: &NetworkContext,
) -> ForkliftResult<bool> {
    //check file existence
    let (src_mode, dest_mode) = match (src.metadata(), dest.metadata()) {
        (None, _) => {
            error!("Source File does not Exist");
            return Err(ForkliftError::FSError(
                "Source File does not exist".to_string(),
            ));
        }
        (_, None) => {
            debug!("Dest File does not exist");
            return Ok(true);
        }
        (Some(src_stat), Some(dest_stat)) => (src_stat.mode(), dest_stat.mode()),
    };

    match (src_context, dest_context) {
        (NetworkContext::Nfs(_), NetworkContext::Nfs(_)) => {
            trace!("src mode {:?}, dest mode {:?}", src_mode, dest_mode);
            return Ok(src_mode != dest_mode);
        }
        (NetworkContext::Samba(ctx), NetworkContext::Samba(dctx)) => {
            let xattr = SmbcXAttr::DosAttr(SmbcDosAttr::Mode);
            let err = "get the dos mode failed";
            let suc = "dos mode retrieved!";

            let src_mod_values = get_xattr(src.path(), ctx, &xattr, err, suc)?;
            let dest_mod_values = get_xattr(dest.path(), dctx, &xattr, err, suc)?;
            trace!(
                "src dos mode {:?}, dest dos mode {:?}",
                src_mod_values,
                dest_mod_values
            );
            Ok(src_mod_values != dest_mod_values)
        }
        (_, _) => {
            error!("Filesystems do not match!");
            return Err(ForkliftError::FSError(
                "Filesystems do not match!".to_string(),
            ));
        }
    }
}

///
/// create an empty vector to store the name of a symlink
///
/// @param size     the length of the name of the target file
///
/// @param readmax  the maximum read length
///
/// @return         returns the target link as a vector of ubytes
///
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

///
/// read a symlink into a String
///
/// @param path     the path to the link file
///
/// @param size     the length of the name of the link's target file
///
/// @param readmax  the maximum read length
///
/// @param context  the filesystem context
///
/// @return         returns a String containing the name of the target file
///
fn read_link(path: &Path, size: i64, context: &Nfs) -> ForkliftResult<String> {
    let readmax = context.get_readmax()?;
    let mut src_target: Vec<u8> = make_target(size, readmax)?;
    if let Err(e) = context.readlink(path, &mut src_target) {
        let err = format!("Unable to read link at {}, {:?}", path.to_string_lossy(), e);
        error!("{}", err);
        return Err(ForkliftError::FSError(err));
    }
    Ok(String::from_utf8(src_target)?)
}

///
/// check if the destination symlink links to the same file as the source, copy if not
///
/// @param src          Source file entry
///
/// @param src_context  the context of the source filesystem
///
/// @param dest         Dest file entry
///
/// @param dest_context the context of the destination filesystem
///
/// @return return the outcome of the Sync (either Update or create) or Error
///
/// @note Samba does not support symlinks, so copy_link will immediately return
///       with an error
///
pub fn copy_link(
    src: &Entry,
    src_context: &NetworkContext,
    dest: &Entry,
    dest_context: &NetworkContext,
) -> ForkliftResult<SyncOutcome> {
    //Check if correct Filesytem
    let (context, dcontext) = match (src_context, dest_context) {
        (NetworkContext::Nfs(ctx), NetworkContext::Nfs(dctx)) => (ctx, dctx),
        (_, _) => {
            return Err(ForkliftError::FSError(
                "Samba does not support symlinks".to_string(),
            ));
        }
    };
    //Check if files exist....
    let (src_size, dest_size) = match (src.metadata(), dest.metadata()) {
        (None, _) => {
            return Err(ForkliftError::FSError(
                "Source File does not exist!".to_string(),
            ));
        }
        (Some(src_stat), None) => (src_stat.size(), 0),
        (Some(src_stat), Some(dest_stat)) => (src_stat.size(), dest_stat.size()),
    };
    let (src_path, dest_path) = (src.path(), dest.path());
    let src_target = read_link(src_path, src_size, context)?;
    let outcome: SyncOutcome;

    match dest.is_link() {
        Some(true) => {
            let dest_target = read_link(dest_path, dest_size, dcontext)?;
            if dest_target != src_target {
                match dcontext.unlink(dest_path) {
                    Ok(_) => (),
                    Err(e) => {
                        let err = format!(
                            "Could not remove {:?} while updating link, {}",
                            dest_path, e
                        );
                        error!("{}", err);
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
            error!("{}", err);
            return Err(ForkliftError::FSError(err));
        }
        None => {
            outcome = SyncOutcome::SymlinkCreated;
        }
    }

    //create new symlink
    match dcontext.symlink(Path::new(&src_target), dest_path) {
        Ok(_) => (),
        Err(e) => {
            let err = format!(
                "Error {}, Could not create link from {:?} to {:?}",
                e, dest_path, src_target
            );
            error!("{}", err);
            return Err(ForkliftError::FSError(err));
        }
    };
    Ok(outcome)
}

///
/// read as much data from a file from the offset as possible in one pass
///
/// @param file     The file to be read
///
/// @param offset   The location where the read will start
///
/// @param path     the path of the file
///
/// @return         a vector of ubytes containing the data in the file
///
/// @note           while this function will attempt to read BUFF_SIZE
///                 bytes from the file starting from offset, it is
///                 still possible that the actual number of bytes read is
///                 smaller.  Be sure to check the length of the returned
///                 vector for the true number of bytes read
///
fn read_chunk(file: &FileType, offset: u64, path: &Path) -> ForkliftResult<Vec<u8>> {
    match file.read(BUFF_SIZE, offset) {
        Ok(buf) => Ok(buf),
        Err(e) => {
            let err = format!(
                "Error {:?}, Could not read from {}",
                e,
                path.to_string_lossy(),
            );
            error!("{}", err);
            Err(ForkliftError::FSError(err))
        }
    }
}

///
/// open the file at path in context.
///
/// @param path     the path of the file to be opened
///
/// @param context  the filesystem context of the file
///
/// @param flags    the flags used when opening the file
///
/// @param error    the error message should the file fail to open
///
/// @return         a FileType containing the opened file
///
/// @note           since mode is never used, we can set mode to be anything
///
fn open_file(
    path: &Path,
    context: &mut NetworkContext,
    flags: OFlag,
    error: &str,
) -> ForkliftResult<FileType> {
    match context.open(path, flags, Mode::S_IRWXU) {
        Ok(f) => Ok(f),
        Err(e) => {
            let err = format!("Error {}, {}", e, error);
            error!("{}", err);
            return Err(ForkliftError::FSError(err));
        }
    }
}

///
/// write buffer to the file at path starting at the offset
///
/// @param path     the path of the file to be written to
///
/// @param file     the FileType holding the file to be
///                 written to
///
/// @param buffer   the vector containing the bytes to be
///                 written
///
/// @param offset   the place in file where the write starts
///
/// @return         the number of bytes written to the file
///                 is returned
///
/// @note           sometimes the size of the input buffer is too
///                 big to write, so we can check the num written
///                 against the size of the buffer to see if we
///                 need to write again
///
fn write_file(path: &Path, file: &FileType, buffer: &[u8], offset: u64) -> ForkliftResult<i32> {
    match file.write(buffer, offset) {
        Ok(n) => Ok(n),
        Err(e) => {
            let err = format!("Error {}, Could not write to {:?}", e, path);
            error!("{}", err);
            return Err(ForkliftError::FSError(err));
        }
    }
}

///
/// copy an entry from source to destination
///
/// @param src          Source file entry
///
/// @param src_context  the context of the source filesystem
///
/// @param dest         Dest file entry
///
/// @param dest_context the context of the destination filesystem
///
/// @return             the sync outcome File Copied or an error
///
pub fn copy_entry(
    src: &Entry,
    src_context: &mut NetworkContext,
    dest: &Entry,
    dest_context: &mut NetworkContext,
) -> ForkliftResult<SyncOutcome> {
    //check if src exists (which it should...)
    let (src_path, dest_path) = (src.path(), dest.path());
    if !exist(src_path, src_context) {
        let err = format!("Source file {:?} should exist!", src_path);
        error!("{}", err);
        return Err(ForkliftError::FSError(err));
    }
    let err = format!("Could not open {:?} for reading", src_path);
    let src_file = open_file(src_path, src_context, OFlag::O_RDONLY, &err)?;
    let flags = OFlag::O_RDWR | OFlag::O_CREAT;
    let dest_file = match dest_context.create(&dest_path, flags, Mode::S_IRWXU) {
        Ok(f) => f,
        Err(e) => {
            error!("Error {:?}", e);
            let err = format!("Could not open {:?} for writing", dest_path);
            error!("{}", err);
            return Err(ForkliftError::FSError(err));
        }
    };

    let mut offset = 0;
    let mut end = false;
    while { !end } {
        let buffer = read_chunk(&src_file, offset, src_path)?;
        let num_written = write_file(dest_path, &dest_file, &buffer, offset)?;
        if num_written == 0 {
            end = true;
        }
        offset = offset + num_written as u64;
        //INSERT PROGRESS MESSAGE HERE
        //SEND PROGRESS
    }
    Ok(SyncOutcome::FileCopied)
}

///
/// checksums a destination file, copying over the data from the src file in the chunks
/// where checksum fails
///
/// @param src          Source file entry
///
/// @param src_context  the context of the source filesystem
///
/// @param dest         Dest file entry
///
/// @param dest_context the context of the destination filesystem
///
/// @return             the sync outcome File Updated or Up To Date,
///                     otherwise an error occured
///
pub fn checksum_copy(
    src: &Entry,
    src_context: &mut NetworkContext,
    dest: &Entry,
    dest_context: &mut NetworkContext,
) -> ForkliftResult<SyncOutcome> {
    let (src_path, dest_path) = (src.path(), dest.path());
    //check if src exists (which it should...)
    if !exist(src_path, src_context) {
        let err = format!("Source file {:?} should exist", src_path);
        error!("{}", err);
        return Err(ForkliftError::FSError(err));
    }
    // open src and dest files
    let err = format!("Could not open {:?} for reading", src_path);
    let src_file = open_file(src_path, src_context, OFlag::O_RDONLY, &err)?;
    let flags = OFlag::O_RDWR | OFlag::O_CREAT;
    let dest_file = open_file(dest_path, dest_context, flags, &err)?;

    //loop until end, count the number of times we needed to update the file
    let (mut offset, mut counter) = (0, 0);
    let mut meowhash = MeowHasher::new();
    let mut end = false;
    let mut file_buf: Vec<u8> = vec![];
    while { !end } {
        let mut num_written: i32 = 0;
        //read 1M from src and hash it
        let mut src_buf = read_chunk(&src_file, offset, src_path)?;
        meowhash.input(&src_buf);
        let hash_src = meowhash.result_reset();
        //read 1M from dest and hash it
        let dest_buf = read_chunk(&dest_file, offset, dest_path)?;
        meowhash.input(&dest_buf);
        let hash_dest = meowhash.result_reset();

        if hash_src != hash_dest {
            if src_buf.len() < dest_buf.len() {
                dest_file.truncate(src_buf.len() as u64)?;
            }
            //write src_buf -> dest
            num_written = write_file(dest_path, &dest_file, &src_buf, offset)?;
            counter += 1;
        }
        //update offset, add bytes to file_buf and check if num_written > 0
        if num_written > 0 {
            src_buf.truncate(num_written as usize);
            file_buf.append(&mut src_buf);
            offset += (num_written - 1) as u64;
        } else {
            file_buf.append(&mut src_buf);
            offset += src_buf.len() as u64;
        }
        if src_buf.len() == 0 {
            end = true;
        }
    } //end loop
    meowhash.input(&file_buf);
    // send this value
    let _whole_checksum = meowhash.result();
    if counter == 0 {
        return Ok(SyncOutcome::UpToDate);
    }
    Ok(SyncOutcome::ChecksumUpdated)
}

///don't forget to add a progress sender at some point......
///Also update the function once you add progress sending...
///
/// syncs the src and dest files.  It also sends the current progress
/// of the rsync of the entry.
///
/// @param src          Source file entry
///
/// @param src_context  the context of the source filesystem
///
/// @param dest         Dest file entry
///
/// @param dest_context the context of the destination filesystem
///
/// @return             the outcome of the entry rsync (or a ForkliftError if it fails)
///
pub fn sync_entry(
    src: &Entry,
    src_context: &mut NetworkContext,
    dest: &Entry,
    dest_context: &mut NetworkContext,
) -> ForkliftResult<SyncOutcome> {
    match src.is_link() {
        Some(true) => {
            trace!("Is link!");
            return copy_link(src, src_context, dest, dest_context);
        }
        Some(false) => (),
        None => {
            let err = format!("Source file {:?} is_link should not be None!", src.path());
            error!("{}", err);
            return Err(ForkliftError::FSError(err));
        }
    }

    match src.is_dir() {
        Some(true) => {
            trace!("Is directory!");
            return make_dir(src.path(), src_context, dest.path(), dest_context);
        }
        Some(false) => (),
        None => {
            let err = format!("Source file {:?} is_dir should not be None!", src.path());
            error!("{}", err);
            return Err(ForkliftError::FSError(err));
        }
    }

    //check if size different or if src more recent than dest
    match (has_different_size(src, dest), is_more_recent(src, dest)) {
        (Ok(size_dif), Ok(recent)) => {
            if size_dif || recent {
                debug!("Is different!!! size {}  recent {}", size_dif, recent);
                return copy_entry(src, src_context, dest, dest_context);
            } else {
                return checksum_copy(src, src_context, dest, dest_context);
            }
        }
        (Err(e), _) => {
            return Err(e);
        }
        (_, Err(e)) => {
            return Err(e);
        }
    };
}

///
/// given a source sid, check through a list of destination acls for the acl
/// with the matching destination sid, remove it from the list and return it
///
/// @param check_sid    the source sid to check for
///
/// @param dest_acls    the list of destination acls
///
/// @return             return Some<ACE>, where ACE is the acl in the list
///                     of destination acls with the same sid as check_sid
///                     otherwise if dest_acls does not have an acl with the
///                     input sid
///
fn check_acl_sid_remove(check_sid: &Sid, dest_acls: &mut Vec<SmbcAclValue>) -> Option<ACE> {
    let mut count = 0;
    for dest_acl in dest_acls.clone() {
        match dest_acl {
            SmbcAclValue::Acl(ACE::Numeric(
                SidType::Numeric(Some(dest_sid)),
                atype,
                flag,
                mask,
            )) => {
                debug!("Sid to check {}, dest sid {}", *check_sid, &dest_sid);
                if *check_sid == dest_sid {
                    let ret = ACE::Numeric(SidType::Numeric(Some(dest_sid)), atype, flag, mask);
                    &dest_acls.remove(count);
                    return Some(ret);
                }
            }
            _ => (),
        }
        count += 1;
    }
    None
}

///
/// Change the Dos Mode Attribute of the destination file to match
/// the source Dos Mode Attribute
///
/// @param src_ctx      the Samba context of the source filesystem
///
/// @param dest_ctx     the Samba context of the destination filesystem
///
/// @param src_path     the path to the source file
///
/// @param dest_path    the path to the destination file
///
/// @return             Nothing, or an error should any of the xattr
///                     functions fail.
///
fn change_mode(
    src_ctx: &Smbc,
    dest_ctx: &Smbc,
    src_path: &Path,
    dest_path: &Path,
) -> ForkliftResult<()> {
    let mode_xattr = SmbcXAttr::DosAttr(SmbcDosAttr::Mode);
    let err = format!("unable to retrieve dos mode from file {:?}", src_path);
    let suc = "retrieved dos mode!";

    let mut m = get_xattr(src_path, src_ctx, &mode_xattr, &err, suc)?;
    m.pop(); //remove ending \u{0}
    let dosmode = match xattr_parser(CompleteByteSlice(&m)) {
        Ok((_, mode)) => mode,
        Err(e) => {
            let err = format!(
                "Error {}, unable to parse xattr from file {:?}",
                e, src_path
            );
            error!("{}", err);
            return Err(ForkliftError::FSError(err));
        }
    };
    trace!("dosmode src is {:?}", &dosmode);
    let err = format!(
        "unable to set dos mode {} for file {:?}",
        &dosmode, dest_path
    );
    let suc = format!("set file {:?} dosmode to {}", dest_path, &dosmode);

    set_xattr(dest_path, dest_ctx, &mode_xattr, &dosmode, &err, &suc)?;
    Ok(())
}

///
/// get the numeric destination SID corresponding to a named
/// source SID.
///
/// @param sid  The named source SID
///
/// @param dest_acls_plus A vector of all of the destination acls named
///
/// @param dest_ctx The filesystem context of the destination filesystem
///
/// @param dest_path The path of the file whose acl's you are checking
///
/// @return Option<Sid>  Some(SID) if a matching SID was found, NONE if
///                      the file does not have a named equivalent ACL.
///
fn get_mapped_sid(
    sid: &str,
    dest_acls_plus: &Vec<SmbcAclValue>,
    dest_ctx: &Smbc,
    dest_path: &Path,
) -> ForkliftResult<Option<Sid>> {
    //let dest_acls = get_acl_list(dest_ctx, dest_path, false);
    let mut count = 0;
    for ace in dest_acls_plus {
        match ace {
            SmbcAclValue::AclPlus(ACE::Named(SidType::Named(Some(dest_sid)), _, _, _)) => {
                debug!("src sid {} dest_sid {}", &sid, &dest_sid);
                if sid == dest_sid {
                    trace!("equals src sid {} dest_sid {}", &sid, &dest_sid);
                    let dest_acls = get_acl_list(dest_ctx, dest_path, false)?;
                    let send_ace = &dest_acls[count];
                    match send_ace {
                        SmbcAclValue::Acl(ACE::Numeric(SidType::Numeric(Some(send)), _, _, _)) => {
                            return Ok(Some(send.clone()));
                        }
                        _ => (),
                    }
                }
            }
            _ => (),
        }
        count += 1;
    }
    Ok(None)
}

///
/// Temporarily map a named acl from the source filesystem to a file at dest_path
/// in order to determine the numeric equivalent of the mapped sid in the destination
/// file, then remove the temporary acl
///
/// @param sid          the named source acl to map
///
/// @param dest_ctx     the Samba context of the destination filesystem
///
/// @param dest_apth    the path do the destination file
///
/// @return             the numeric sid of the mapped source sid
///                     return a forklift error should any of the xattr
///                     functions fail.
///
fn map_temp_acl(sid: &str, dest_ctx: &Smbc, dest_path: &Path) -> ForkliftResult<Sid> {
    let temp_ace = ACE::Named(
        SidType::Named(Some(sid.to_string())),
        AceAtype::ALLOWED,
        AceFlag::NONE,
        "FULL".to_string(),
    );
    let err = format!("unable to set a temp acl {}", &temp_ace);
    let suc = format!("set a temp acl {} success", &temp_ace);
    let xattr_set = SmbcXAttr::AclAttr(SmbcAclAttr::AclNonePlus);
    let val = SmbcXAttrValue::Ace(temp_ace.clone());
    set_xattr(dest_path, dest_ctx, &xattr_set, &val, &err, &suc)?;

    let dest_acls = get_acl_list(dest_ctx, dest_path, true)?;
    let ret = match get_mapped_sid(&sid, &dest_acls, dest_ctx, dest_path) {
        Ok(Some(dest_sid)) => dest_sid,
        Ok(None) => {
            let err = format!("Unsucessful in mapping sid {}", &sid);
            error!("{}", err);
            return Err(ForkliftError::FSError(err));
        }
        Err(e) => return Err(e),
    };
    match dest_ctx.removexattr(
        dest_path,
        &SmbcXAttr::AclAttr(SmbcAclAttr::AclPlus(temp_ace)),
    ) {
        Ok(_) => debug!("removing temp acl success"),
        Err(e) => {
            let err = format!("Error {}, failed to remove temp acl", e);
            error!("{}", err);
            return Err(ForkliftError::FSError(err));
        }
    }
    Ok(ret)
}

///
/// remove the old acl from the destiation file and set a new one to replace
/// it with a new acl
///
/// @param new_acl      the acl we are replace the old one with
///
/// @param old_acl      the acl we are replacing
///
/// @param dest_ctx     the destination Samba context of the filesystem
///
/// @param dest_path    the destination file we are replacing the acls of
///
/// @return             nothing, or an error should remove or set xattr fail
///
fn replace_acl(
    new_acl: &ACE,
    old_acl: &ACE,
    dest_ctx: &Smbc,
    dest_path: &Path,
) -> ForkliftResult<()> {
    //remove the old acl first
    match dest_ctx.removexattr(
        dest_path,
        &SmbcXAttr::AclAttr(SmbcAclAttr::Acl(old_acl.clone())),
    ) {
        Ok(_) => debug!("Removed old acl {}", old_acl),
        Err(e) => {
            let err = format!("Error {}, failed to remove the old acl {}", e, old_acl);
            error!("{}", err);
            return Err(ForkliftError::FSError(err));
        }
    };
    //set new acl
    let xattr = SmbcXAttr::AclAttr(SmbcAclAttr::AclNone);
    let val = SmbcXAttrValue::Ace(new_acl.clone());
    let err = format!("failed to set the new acl {}", new_acl);
    let suc = format!("Set new acl {}", new_acl);
    set_xattr(dest_path, dest_ctx, &xattr, &val, &err, &suc)?;
    Ok(())
}

///
/// map the named source sid to a numeric destination sid
///
/// @param sid          the named source sid
///
/// @param dest_ctx     the Samba context of the destination filesystem
///
/// @param dest_path    the path of the destination file
///
/// @return             return the mapped Sid, or an Error should
///                     the mapping fail
///
fn map_name(sid: &str, dest_ctx: &Smbc, dest_path: &Path) -> ForkliftResult<Sid> {
    let destplus = get_acl_list(dest_ctx, dest_path, true)?;
    match get_mapped_sid(&sid, &destplus, dest_ctx, dest_path) {
        //add the acl to the file and call get_mapped_sid again
        Ok(None) => Ok(map_temp_acl(&sid, dest_ctx, dest_path)?),
        //the acl is in, map a to the returned sid
        Ok(Some(dest_sid)) => Ok(dest_sid),
        Err(e) => Err(e),
    }
}

///
/// Copy the acl from source to destination if they are different
///
/// @param src          The ACE parts of the source acl
///
/// @param dest         The ACE parts of the destination acl
///
/// @param dest_ctx     The Samba context of the destination filesystem
///
/// @param dest_path    The destination filepath
///
/// @return bool        return true if an acl was copied, false if not
///                     otherwise, there was an error in the copy process
///
fn copy_if_diff(
    src: (Sid, AceAtype, AceFlag, XAttrMask),
    dest: (Sid, AceAtype, AceFlag, XAttrMask),
    dest_ctx: &Smbc,
    dest_path: &Path,
) -> ForkliftResult<bool> {
    trace!("dtype {:?}, dflags {:?}, dmask {}", dest.1, dest.2, dest.3);
    trace!("atype {:?}, aflags {:?}, mask {}", src.1, src.2, src.3);
    if src.1 != dest.1 || src.2 != dest.2 || src.3 != dest.3 {
        let (new_acl, old_acl) = (
            ACE::Numeric(SidType::Numeric(Some(src.0)), src.1, src.2, src.3),
            ACE::Numeric(SidType::Numeric(Some(dest.0)), dest.1, dest.2, dest.3),
        );
        debug!("New {}, Old {}", new_acl, old_acl);
        replace_acl(&new_acl, &old_acl, dest_ctx, dest_path)?;
        return Ok(true);
    }
    Ok(false)
}

///
/// check if a source acl needs to be copied to the destination
/// if yes, copy the file, otherwise do nothing
///
/// @param src          The ACE parts of the source acl
///
/// @param dest_acls    The list of destination acls
///
/// @param dest_ctx     The Samba context of the destination filesystem
///
/// @param dest_path    The destination filepath
///
/// @return bool        return true if an acl was copied, false if not
///                     otherwise, there was an error in the copy process
///
fn copy_acl(
    src: (Sid, AceAtype, AceFlag, XAttrMask),
    dest_acls: &mut Vec<SmbcAclValue>,
    dest_ctx: &Smbc,
    dest_path: &Path,
) -> ForkliftResult<bool> {
    match check_acl_sid_remove(&src.0, dest_acls) {
        //check if same
        Some(ACE::Numeric(SidType::Numeric(Some(dest_sid)), dtype, dflags, dmask)) => {
            // if there any differences, copy
            copy_if_diff(src, (dest_sid, dtype, dflags, dmask), dest_ctx, dest_path)
        } //check if same here, if so, do nothing, else copy
        //does not exist, so copy (only set needed, nothing to remove)
        _ => {
            let new_acl = ACE::Numeric(SidType::Numeric(Some(src.0)), src.1, src.2, src.3);
            trace!("New Acl {:?}", new_acl);
            let xattr_set = SmbcXAttr::AclAttr(SmbcAclAttr::AclNone);
            let val = SmbcXAttrValue::Ace(new_acl);
            let err = "failed to copy new acl";
            let suc = "Copied new acl";
            set_xattr(dest_path, dest_ctx, &xattr_set, &val, err, suc)?;
            Ok(true)
        }
    }
}

/*
   map named acl Sids from a src file to their destination sids
*/
///
/// map named acl Sids from a source file to their destinarion numeric Sids
/// then replace the incorrect destination acls with the source acls
///
pub fn map_names_and_copy(
    src_acls: &Vec<SmbcAclValue>,
    src_acls_plus: &Vec<SmbcAclValue>,
    dest_acls: &mut Vec<SmbcAclValue>,
    dest_ctx: &Smbc,
    dest_path: &Path,
) -> ForkliftResult<bool> {
    let mut map = match NAME_MAP.lock() {
        Ok(hm) => hm,
        Err(_) => {
            return Err(ForkliftError::FSError(
                "Could not get sid name map".to_string(),
            ))
        }
    };
    let (mut copied, mut count) = (false, 0);
    for src_acl in src_acls_plus {
        match (src_acl.clone(), src_acls[count].clone()) {
            (
                SmbcAclValue::AclPlus(ACE::Named(SidType::Named(Some(sid)), _, _, _)),
                SmbcAclValue::Acl(ACE::Numeric(SidType::Numeric(Some(_)), atype, aflags, mask)),
            ) => {
                //technically this part is the map names...
                //check if sid is mapped, add to map if not already in
                let mapped = match map.entry(sid.clone()) {
                    E::Occupied(o) => o.into_mut(),
                    E::Vacant(v) => v.insert(map_name(&sid, dest_ctx, dest_path)?),
                };
                copied = copy_acl(
                    (mapped.clone(), atype, aflags, mask),
                    dest_acls,
                    dest_ctx,
                    dest_path,
                )?;
            }
            (_, _) => {
                return Err(ForkliftError::FSError(
                    "input src acls are not formatted correctly!!".to_string(),
                ))
            }
        }
        count += 1;
    }
    for dest_acl in dest_acls.clone() {
        match dest_acl {
            SmbcAclValue::Acl(ace) => match dest_ctx.removexattr(
                dest_path,
                &SmbcXAttr::AclAttr(SmbcAclAttr::Acl(ace.clone())),
            ) {
                Ok(_) => debug!("Removed extra acl {}", ace),
                Err(e) => {
                    let err = format!("Error {}, failed to remove the old acl {}", e, ace);
                    error!("{}", err);
                    return Err(ForkliftError::FSError(err));
                }
            },
            _ => {
                return Err(ForkliftError::FSError(
                    "input dest acls are not formatted correctly!".to_string(),
                ))
            }
        }
    }
    trace!("{:?}", *map);
    Ok(copied || !dest_acls.is_empty())
}

///
/// get list of acl values
/// @param ctx  the context of the filesystem
///
/// @param path the path of the file to grab the xattrs from
///
/// @param plus boolean value denoting whether the returned list is named
///             or numeric
///
/// @return     return the list of acls of the input file
///
pub fn get_acl_list(fs: &Smbc, path: &Path, plus: bool) -> ForkliftResult<Vec<SmbcAclValue>> {
    let err = format!("unable to get acls from {:?}", path);
    let suc = format!("acl all get success");
    let mut acls = match plus {
        true => {
            let acl_plus_xattr = SmbcXAttr::AclAttr(SmbcAclAttr::AclAllPlus);
            get_xattr(path, fs, &acl_plus_xattr, &err, &suc)?
        }
        false => {
            let acl_xattr = SmbcXAttr::AclAttr(SmbcAclAttr::AclAll);
            get_xattr(path, fs, &acl_xattr, &err, &suc)?
        }
    };
    acls.pop();
    let acl_list = match xattr_parser(CompleteByteSlice(&acls)) {
        Ok((_, acl_list)) => acl_list,
        Err(e) => {
            let err = format!(
                "Error {}, unable to parse acls {}",
                e,
                String::from_utf8_lossy(&acls)
            );
            error!("{}", err);
            return Err(ForkliftError::FSError(err));
        }
    };
    match acl_list {
        SmbcXAttrValue::AclAll(s_acls) => Ok(s_acls),
        _ => Ok(vec![]),
    }
}

///
/// change the destination linux stat mode to that of the source file
///
/// @param context  the filesystem context of the destination file
///
/// @param path     the path to the destination file
///
/// @param mode     the source mode
///
/// @return         Nothing if successful, otherwise an error is raised
///
/// @note           In a Samba Context, chmod will change the Dos Mode
///                 (it's necessary for Normal Dos Mode, otherwise
///                 a normal Dos Mode will be treated as an Archive file)
///                 However, it may not change the linux stat mode
///                 correctly (depends on your config file, see Smbc for
///                 details)
///
fn change_stat_mode(context: &NetworkContext, path: &Path, mode: u32) -> ForkliftResult<()> {
    match context.chmod(path, Mode::from_bits_truncate(mode)) {
        Ok(_) => {
            debug!("Chmod of file {:?} to {} ran", path, mode);
            Ok(())
        }
        Err(e) => {
            let err = format!("Error {}, chmod failed", e);
            error!("{}", err);
            Err(ForkliftError::FSError(err))
        }
    }
}

///
/// Copy the source permissions to the destination file and return the
/// current status of the rsync (specifically of the permissions)
///
/// @param src          Source file entry
///
/// @param src_context  the context of the source filesystem
///
/// @param dest         Dest file entry
///
/// @param dest_context the context of the destination filesystem
///
/// @return             the outcome of the permission rsync (or a ForkliftError if it fails)
///
pub fn copy_permissions(
    src: &Entry,
    src_context: &NetworkContext,
    dest: &Entry,
    dest_context: &NetworkContext,
) -> ForkliftResult<SyncOutcome> {
    let src_mode = match (src.is_link(), src.metadata()) {
        (Some(true), _) => return Ok(SyncOutcome::UpToDate),
        (None, _) => {
            let err = format!("is_link was None for {:?}", src.path());
            error!("{}", err);
            return Err(ForkliftError::FSError(err));
        }
        (_, None) => {
            let err = format!("src file does not exist for {:?}", src.path());
            error!("{}", err);
            return Err(ForkliftError::FSError(err));
        }
        (Some(false), Some(stat)) => stat.mode(),
    };

    let (src_path, dest_path) = (src.path(), dest.path());
    let outcome;

    match (src_context, dest_context) {
        //stat mode diff
        (NetworkContext::Nfs(_), NetworkContext::Nfs(_)) => {
            match has_different_permissions(src, src_context, dest, dest_context) {
                Ok(true) => {
                    change_stat_mode(dest_context, dest_path, src_mode)?;
                    outcome = SyncOutcome::PermissionsUpdated;
                }
                Ok(false) => outcome = SyncOutcome::UpToDate,
                Err(e) => {
                    return Err(e);
                }
            }
        }
        //dos mode diff
        (NetworkContext::Samba(src_ctx), NetworkContext::Samba(dest_ctx)) => {
            let copied = map_names_and_copy(
                &get_acl_list(src_ctx, src_path, false)?,
                &get_acl_list(src_ctx, src_path, true)?,
                &mut get_acl_list(dest_ctx, dest_path, false)?,
                dest_ctx,
                dest_path,
            )?;
            match has_different_permissions(src, src_context, dest, dest_context) {
                Ok(true) => {
                    trace!("src mode {}", src_mode);
                    change_stat_mode(dest_context, dest_path, src_mode)?;
                    change_mode(src_ctx, dest_ctx, src_path, dest_path)?;
                    outcome = SyncOutcome::PermissionsUpdated
                }
                Ok(false) => {
                    if copied {
                        outcome = SyncOutcome::PermissionsUpdated;
                    } else {
                        outcome = SyncOutcome::UpToDate
                    }
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        (_, _) => return Err(ForkliftError::FSError("Different contexts!".to_string())),
    }

    Ok(outcome)
}
