use ::smbc::*;
use crossbeam::channel::Sender;
use digest::Digest;
use lazy_static::lazy_static;
use libnfs::*;
use log::*;
use meowhash::*;
use nix::fcntl::OFlag;
use nix::sys::stat::Mode;
use nom::types::CompleteByteSlice;
use pathdiff::*;

use std::collections::hash_map::Entry as E;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::error::{ForkliftError, ForkliftResult};
use crate::filesystem::*;
use crate::filesystem_entry::Entry;
use crate::progress_message::ProgressMessage;

/// default buffer size
const BUFF_SIZE: u64 = 1024 * 1000;

lazy_static! {
    /// singleton containing a map of named ID's to Sid's
    pub static ref NAME_MAP: Mutex<HashMap<String, Sid>> = {
        let mut map = HashMap::new();
        map.insert("\\Everyone".to_string(), Sid(vec![1, 0]));
        map.insert("\\Creator Owner".to_string(), Sid(vec![3, 0]));
        map.insert("\\Creator Group".to_string(), Sid(vec![3, 1]));
        Mutex::new(map)
    };
}

#[derive(PartialEq, Debug, Clone)]
/// enum denoting the outcome of a sync_entry
pub enum SyncOutcome {
    /// file/directory is up-to-date
    UpToDate,
    /// copied a file
    FileCopied,
    /// updated a symlink
    SymlinkUpdated,
    /// created a symlink
    SymlinkCreated,
    /// symlink destination does not exist/is not in share
    SymlinkSkipped,
    /// updated the permissions (ACL or otherwise)
    PermissionsUpdated,
    /// copied a directory
    DirectoryCreated,
    /// updated a directory internal bytes
    DirectoryUpdated,
    /// updated a file internal bytes
    ChecksumUpdated(Vec<u8>),
}

///
/// checks if a path is valid
///
/// @param path     The path to be checked
///
/// @param context  The filesystem context the path is checked against
///
/// @return         true if the path exists (is valid), false otherwise
///
pub fn exist(path: &Path, context: &mut NetworkContext) -> bool {
    context.stat(path).is_ok()
}

///
/// gets the relative path (the parts of the path in common)
///
/// @param base_path    the base path
///
/// @param comp_path    the comparison path
///
/// @return     the relative path between base and comp, error if
///             a relative path does not exist
///
pub fn get_rel_path(base_path: &Path, comp_path: &Path) -> ForkliftResult<PathBuf> {
    match diff_paths(&base_path, &comp_path) {
        None => {
            let err = format!(
                "Could not get relative path from {:?} to {:?}",
                &base_path, &comp_path
            );
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
/// @param context  the Samba filesystem of the file
///
/// @param attr     The attribute being set.  valid descriptors can be
///                 found in Smbc.
///
/// @param value      The value to be set in the external attribute
///
/// @param error    The error description should the set fail
///
/// @param success  The string to be printed to debug logs upon success
///
/// @note           See Smbc.rs for notes on setxattr
///
pub fn set_xattr(
    path: &Path,
    context: &Smbc,
    attr: &SmbcXAttr,
    value: &SmbcXAttrValue,
    error: &str,
    success: &str,
) -> ForkliftResult<()> {
    match context.setxattr(path, attr, value, XAttrFlags::SMBC_XATTR_FLAG_CREATE) {
        Ok(_) => {
            trace!("set success! {}", success);
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
/// @param context  the Samba filesystem of the file
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
///                 Also, all returned vectors should end in the \{0},
///                 or Null character.  If you want to parse this using
///                 xattr_parser, you will have to pop off the null
///                 terminator.
///
/// @return         The attributes as a Vec<u8>, or error should the
///                 function fail
///
pub fn get_xattr(
    path: &Path,
    context: &Smbc,
    attr: &SmbcXAttr,
    error: &str,
    success: &str,
) -> ForkliftResult<Vec<u8>> {
    match context.getxattr(path, attr) {
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
/// make and/or update a directory endpoint in the destination filesystem,
/// keeping Dos or Unix permissions (depending on if the context is Samba or NFS)
///
/// @param src_path     the path of the equivalent directory from the source filesystem
///
/// @param dest_path    the path of the directory being created
///
/// @param src_context  the source filesystem
///
/// @param dest_context the destination filesystem
///
/// @return             returns the Sync outcome (or an error)
///
/// @note           If the filesystem context is Samba CIFS, then please note
///                 that the mode of the directory cannot go below 555 (see chmod
///                 notes in Smbc)
///
pub fn make_dir(
    src_path: &Path,
    dest_path: &Path,
    src_context: &mut NetworkContext,
    dest_context: &mut NetworkContext,
) -> ForkliftResult<SyncOutcome> {
    let outcome: SyncOutcome;
    let exists = exist(dest_path, dest_context);
    if !exists {
        if let Err(e) = dest_context.mkdir(dest_path) {
            let err = format!(
                "Error {}, Could not create {:?}, exists: {:?}",
                e, dest_path, exists
            );
            error!("{}", err);
            return Ok(SyncOutcome::UpToDate);
        }
    }
    let (src_entry, dest_entry) = (
        Entry::new(&src_path, src_context),
        Entry::new(&dest_path, dest_context),
    );
    // make sure permissions match
    let out = match copy_permissions(&src_entry, &dest_entry, &src_context, &dest_context) {
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
/// @param src_context  the source filesystem
///
/// @param dest_context the destination filesystem
///
/// @return             Nothing on success, Error should the function fail
///                     
pub fn make_dir_all(
    dest_path: &Path,
    src_path: &Path,
    root: &Path,
    src_context: &mut NetworkContext,
    dest_context: &mut NetworkContext,
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
        trace!("dest_parent {:?} , root {:?}", dest_parent, root);
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
        if !exist(&path, dest_context) {
            match make_dir(srcpath, &path, src_context, dest_context) {
                Ok(_) => debug!("made dir {:?}", path),
                Err(e) => {
                    return Err(e);
                }
            };
        }
    }
    Ok(())
}

///
/// Check if the source and dest entries have the same size
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
/// Check if the source entry is more recent than the dest entry
/// Since time attributes remain the same for samba + nfs calls,
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
        (Some(src_stat), Some(dest_stat)) => Ok(src_stat.mtime().num_microseconds()
            > dest_stat.mtime().num_microseconds()
            && src_stat.mtime().num_seconds() > dest_stat.mtime().num_seconds()),
        (None, _) => {
            let err = format!("Source File {:?} does not exist", src.path());
            error!("{}", err);
            Err(ForkliftError::FSError(err))
        }
        (_, None) => {
            trace!("Dest File does not exist");
            Ok(true)
        }
    }
}

///
/// this functions checks whether or not the destination file has the same
/// permission settings as the source file.
///
/// @param src          Source file entry
///
/// @param dest         Dest file entry
///
/// @param src_context  the context of the source filesystem
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
    dest: &Entry,
    src_context: &NetworkContext,
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
            Ok(src_mode != dest_mode)
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
            Err(ForkliftError::FSError(
                "Filesystems do not match!".to_string(),
            ))
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
/// @param context  the filesystem context
///
/// @param size     the length of the name of the link's target file
///
/// @return         returns a String containing the name of the target file
///
fn read_link(path: &Path, context: &Nfs, size: i64) -> ForkliftResult<String> {
    let mut src_target: Vec<u8> = make_target(size, BUFF_SIZE)?;
    if let Err(e) = context.readlink(path, &mut src_target) {
        let err = format!("Unable to read link at {:?}, {:?}", path, e);
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
/// @param dest         Dest file entry
///
/// @param src_context  the context of the source filesystem
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
    dest: &Entry,
    src_context: &mut NetworkContext,
    dest_context: &mut NetworkContext,
) -> ForkliftResult<SyncOutcome> {
    //Check if correct Filesytem
    let (context, dcontext) = match (src_context.clone(), dest_context.clone()) {
        (NetworkContext::Nfs(ctx), NetworkContext::Nfs(dctx)) => (ctx, dctx),
        (_, _) => {
            return Err(ForkliftError::FSError(
                "Samba does not support symlinks".to_string(),
            ));
        }
    };
    //Check if files exist
    let (src_size, dest_size) = match (src.metadata(), dest.metadata()) {
        (None, _) => {
            return Err(ForkliftError::FSError(
                "Source File does not exist!".to_string(),
            ));
        }
        (Some(src_stat), None) => (src_stat.size() + 1, 0),
        (Some(src_stat), Some(dest_stat)) => (src_stat.size() + 1, dest_stat.size() + 1),
    };
    let (src_path, dest_path) = (src.path(), dest.path());
    let mut src_target = read_link(src_path, &context, src_size)?;
    src_target.pop();
    let mut outcome: SyncOutcome;

    match dest.is_link() {
        Some(true) => {
            let mut dest_target = read_link(dest_path, &dcontext, dest_size)?;
            dest_target.pop();
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
            //Not safe to delete
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
    //create new symlink, if creation fails, skip
    match dcontext.symlink(Path::new(&src_target), dest_path) {
        Ok(_) => (),
        Err(e) => {
            let err = format!(
                "Error {}, Could not create link from {:?} to {:?}",
                e, dest_path, src_target
            );
            error!("{}", err);
            outcome = SyncOutcome::SymlinkSkipped;
        }
    };
    Ok(outcome)
}

///
/// read as much data from a file from the offset as possible in one pass
///
/// @param path     the path of the file
///
/// @param file     The file to be read
///
/// @param offset   The location where the read will start
///
/// @return         a vector of ubytes containing the data in the file
///
/// @note           while this function will attempt to read BUFF_SIZE
///                 bytes from the file starting from offset, it is
///                 still possible that the actual number of bytes read is
///                 smaller.  Be sure to check the length of the returned
///                 vector for the true number of bytes read
///
fn read_chunk(path: &Path, file: &FileType, offset: u64) -> ForkliftResult<Vec<u8>> {
    match file.read(BUFF_SIZE, offset) {
        Ok(buf) => Ok(buf),
        Err(e) => {
            let err = format!("Error {:?}, Could not read from {:?}", e, path,);
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
    match context.open(path, flags, Mode::S_IRWXU | Mode::S_IRWXG | Mode::S_IRWXO) {
        Ok(f) => Ok(f),
        Err(e) => {
            let err = format!("Error {}, {}", e, error);
            error!("{}", err);
            Err(ForkliftError::FSError(err))
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
fn write_file(path: &Path, file: &FileType, buffer: &[u8], offset: u64) -> ForkliftResult<u64> {
    match file.write(buffer, offset) {
        Ok(n) => Ok(n),
        Err(e) => {
            let err = format!("Error {}, Could not write to {:?}", e, path);
            error!("{}", err);
            Err(ForkliftError::FSError(err))
        }
    }
}

///
/// copy an entry from source to destination
///
/// @param progress_sender  Channel to send progress to progress_worker
///
/// @param src              Source file entry
///
/// @param dest             Dest file entry
///
/// @param src_context      the context of the source filesystem
///
/// @param dest_context     the context of the destination filesystem
///
/// @return                 the sync outcome File Copied or an error
///
fn copy_entry(
    progress_sender: &Sender<ProgressMessage>,
    src: &Entry,
    dest: &Entry,
    src_context: &mut NetworkContext,
    dest_context: &mut NetworkContext,
) -> ForkliftResult<SyncOutcome> {
    //check if src exists (which it should...)
    let (src_path, dest_path) = (src.path(), dest.path());
    let src_meta = match src.metadata() {
        None => {
            let err = format!("Source file {:?} should exist!", src_path);
            error!("{}", err);
            return Err(ForkliftError::FSError(err));
        }
        Some(m) => m,
    };
    let err = format!("Could not open {:?} for reading", dest_path);
    let src_file = open_file(src_path, src_context, OFlag::empty(), &err)?;
    let flags = OFlag::O_CREAT;
    let dest_file = match dest_context.create(
        &dest_path,
        flags,
        Mode::S_IRWXU | Mode::S_IRWXO | Mode::S_IRWXG,
    ) {
        Ok(f) => f,
        Err(e) => {
            trace!("Error {:?}", e);
            let err = format!("Could not open {:?} for writing", dest_path);
            return Err(ForkliftError::FSError(err));
        }
    };

    let mut offset = 0;
    let mut end = false;
    while { !end } {
        let buffer = read_chunk(src_path, &src_file, offset)?;
        let num_written = write_file(dest_path, &dest_file, &buffer, offset)?;
        if num_written == 0 {
            end = true;
        }
        offset += num_written as u64;
        //SEND PROGRESS
        let progress = ProgressMessage::Syncing {
            description: src.path().to_string_lossy().to_string(),
            size: src_meta.size() as usize,
            done: num_written as usize,
        };

        if progress_sender.send(progress).is_err() {
            error!("Unable to send progress");
        }
    }
    Ok(SyncOutcome::FileCopied)
}

///
/// checksums a destination file, copying over the data from the src file in the chunks
/// where checksum fails
///
/// @param progress_sender  Channel to set progress to progress_worker
///
/// @param src              Source file entry
///
/// @param dest             Dest file entry
///
/// @param src_context      the context of the source filesystem
///
/// @param dest_context     the context of the destination filesystem
///
/// @return                 the sync outcome Checksum Updated or Up To Date,
///                         otherwise an error occured
///
pub fn checksum_copy(
    progress_sender: &Sender<ProgressMessage>,
    src: &Entry,
    dest: &Entry,
    src_context: &mut NetworkContext,
    dest_context: &mut NetworkContext,
) -> ForkliftResult<SyncOutcome> {
    let (src_path, dest_path) = (src.path(), dest.path());
    //check if src exists (which it should...)
    let src_meta = match src.metadata() {
        None => {
            let err = format!("Source file {:?} should exist!", src_path);
            error!("{}", err);
            return Err(ForkliftError::FSError(err));
        }
        Some(m) => m,
    };
    // open src and dest files
    let err = format!("Could not open {:?} for reading", dest_path);
    let src_file = open_file(src_path, src_context, OFlag::O_RDONLY, &err)?;
    let flags = OFlag::O_CREAT;
    let dest_file = open_file(dest_path, dest_context, flags, &err)?;

    //loop until end, count the number of times we needed to update the file
    let (mut offset, mut counter) = (0, 0);
    let mut meowhash = MeowHasher::new();
    let mut end = false;
    let mut file_buf: Vec<u8> = vec![];
    while { !end } {
        let mut num_written = 0;
        let mut src_buf = read_chunk(src_path, &src_file, offset)?;
        meowhash.input(&src_buf);
        let hash_src = meowhash.result_reset();
        let dest_buf = read_chunk(dest_path, &dest_file, offset)?;
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
            offset += num_written - 1;
        } else {
            file_buf.append(&mut src_buf);
            offset += src_buf.len() as u64;
        }
        if src_buf.is_empty() {
            end = true;
        }
        //send progress
        let progress = ProgressMessage::CheckSyncing {
            description: src.path().to_string_lossy().into_owned(),
            size: src_meta.size() as usize,
            done: offset as usize,
            check_sum: hash_src.as_slice().to_vec(),
        };
        if progress_sender.send(progress).is_err() {
            error!("Unable to send progress");
        }
    }
    meowhash.input(&file_buf);
    // NOTE: send this value for final check
    let whole_checksum = meowhash.result();
    let whole_checksum = whole_checksum.as_slice().to_vec();
    if counter == 0 {
        return Ok(SyncOutcome::UpToDate);
    }
    Ok(SyncOutcome::ChecksumUpdated(whole_checksum))
}

///
/// syncs the src and dest files.  It also sends the current progress
/// of the rsync of the entry.
///
/// @param progress_sender  Channel to send progress to progress_worker
///
/// @param src              Source file entry
///
/// @param dest             Dest file entry
///
/// @param src_context      the context of the source filesystem
///
/// @param dest_context     the context of the destination filesystem
///
/// @return                 the outcome of the entry rsync (or a ForkliftError if it fails)
///
pub fn sync_entry(
    progress_sender: &Sender<ProgressMessage>,
    src: &Entry,
    dest: &Entry,
    src_context: &mut NetworkContext,
    dest_context: &mut NetworkContext,
) -> ForkliftResult<SyncOutcome> {
    let description = src.path().to_string_lossy().into_owned();
    if progress_sender
        .send(ProgressMessage::StartSync(description))
        .is_err()
    {
        error!("Unable to send progress");
    }
    match src.is_link() {
        Some(true) => {
            trace!("Is link!");
            return copy_link(src, dest, src_context, dest_context);
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
            return make_dir(src.path(), dest.path(), src_context, dest_context);
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
                copy_entry(progress_sender, src, dest, src_context, dest_context)
            } else {
                debug!("not diff {} {}", size_dif, recent);
                checksum_copy(progress_sender, src, dest, src_context, dest_context)
            }
        }
        (Err(e), _) => Err(e),
        (_, Err(e)) => Err(e),
    }
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
    for (count, dest_acl) in dest_acls.iter().enumerate() {
        if let SmbcAclValue::Acl(ACE::Numeric(
            SidType::Numeric(Some(dest_sid)),
            atype,
            flag,
            mask,
        )) = dest_acl
        {
            trace!("Sid to check {}, dest sid {}", *check_sid, &dest_sid);
            if check_sid == dest_sid {
                let ret = ACE::Numeric(
                    SidType::Numeric(Some(dest_sid.clone())),
                    atype.clone(),
                    *flag,
                    *mask,
                );
                dest_acls.remove(count);
                return Some(ret);
            }
        }
    }
    None
}

///
/// Change the Dos Mode Attribute of the destination file to match
/// the source Dos Mode Attribute
///
/// @param src_path     the path to the source file
///
/// @param dest_path    the path to the destination file
///
/// @param src_ctx      the Samba context of the source filesystem
///
/// @param dest_ctx     the Samba context of the destination filesystem
///
/// @return             Nothing, or an error should any of the xattr
///                     functions fail.
///
fn change_mode(
    src_path: &Path,
    dest_path: &Path,
    src_ctx: &Smbc,
    dest_ctx: &Smbc,
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
/// @param dest_path The path of the file whose acl's you are checking
///
/// @param sid  The named source SID
///
/// @param dest_acls_plus A vector of all of the destination acls named
///
/// @param dest_ctx The filesystem context of the destination filesystem
///
/// @return Option<Sid>  Some(SID) if a matching SID was found, NONE if
///                      the file does not have a named equivalent ACL.
///
fn get_mapped_sid(
    dest_path: &Path,
    dest_ctx: &Smbc,
    sid: &str,
    dest_acls_plus: &[SmbcAclValue],
) -> ForkliftResult<Option<Sid>> {
    for (count, ace) in dest_acls_plus.iter().enumerate() {
        if let SmbcAclValue::AclPlus(ACE::Named(SidType::Named(Some(dest_sid)), _, _, _)) = ace {
            debug!("src sid {} dest_sid {}", &sid, &dest_sid);
            if sid == dest_sid {
                trace!("equals src sid {} dest_sid {}", sid, dest_sid);
                let dest_acls = get_acl_list(dest_path, dest_ctx, false)?;
                let send_ace = &dest_acls[count];
                if let SmbcAclValue::Acl(ACE::Numeric(SidType::Numeric(Some(send)), _, _, _)) =
                    send_ace
                {
                    return Ok(Some(send.clone()));
                }
            }
        }
    }
    Ok(None)
}

///
/// Temporarily map a named acl from the source filesystem to a file at dest_path
/// in order to determine the numeric equivalent of the mapped sid in the destination
/// file, then remove the temporary acl
///
/// @param dest_apth    the path do the destination file
///
/// @param dest_ctx     the Samba context of the destination filesystem
///
/// @param sid          the named source acl to map
///
/// @return             the numeric sid of the mapped source sid
///                     return a forklift error should any of the xattr
///                     functions fail.
///
fn map_temp_acl(dest_path: &Path, dest_ctx: &Smbc, sid: &str) -> ForkliftResult<Sid> {
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

    let dest_acls = get_acl_list(dest_path, dest_ctx, true)?;
    let ret = match get_mapped_sid(dest_path, dest_ctx, &sid, &dest_acls) {
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
/// @param dest_path    the destination file we are replacing the acls of
///
/// @param dest_ctx     the destination Samba context of the filesystem
///
/// @param new_acl      the acl we are replace the old one with
///
/// @param old_acl      the acl we are replacing
///
/// @return             nothing, or an error should remove or set xattr fail
///
fn replace_acl(
    dest_path: &Path,
    dest_ctx: &Smbc,
    new_acl: &ACE,
    old_acl: &ACE,
) -> ForkliftResult<()> {
    //remove the old acl first
    match dest_ctx.removexattr(
        dest_path,
        &SmbcXAttr::AclAttr(SmbcAclAttr::Acl(old_acl.clone())),
    ) {
        Ok(_) => trace!("Removed old acl {}", old_acl),
        Err(e) => {
            let err = format!(
                "Error {}, failed to remove the old acl {} from {:?}",
                e, old_acl, dest_path
            );
            error!("{}", err);
            return Err(ForkliftError::FSError(err));
        }
    };
    //set new acl
    let xattr = SmbcXAttr::AclAttr(SmbcAclAttr::AclNone);
    let val = SmbcXAttrValue::Ace(new_acl.clone());
    let err = format!("failed to set the new acl {} path {:?}", new_acl, dest_path);
    let suc = format!("Set new acl {}", new_acl);
    set_xattr(dest_path, dest_ctx, &xattr, &val, &err, &suc)?;
    Ok(())
}

///
/// map the named source sid to a numeric destination sid
///
/// @param dest_path    the path of the destination file
///
/// @param dest_ctx     the Samba context of the destination filesystem
///
/// @param sid          the named source sid
///
/// @return             return the mapped Sid, or an Error should
///                     the mapping fail
///
fn map_name(dest_path: &Path, dest_ctx: &Smbc, sid: &str) -> ForkliftResult<Sid> {
    let destplus = get_acl_list(dest_path, dest_ctx, true)?;
    match get_mapped_sid(dest_path, dest_ctx, &sid, &destplus) {
        //add the acl to the file and call get_mapped_sid again
        Ok(None) => Ok(map_temp_acl(dest_path, dest_ctx, &sid)?),
        //the acl is in, map a to the returned sid
        Ok(Some(dest_sid)) => Ok(dest_sid),
        Err(e) => Err(e),
    }
}

///
/// Copy the acl from source to destination if they are different
///
/// @param dest_path    The destination filepath
///
/// @param dest_ctx     The Samba context of the destination filesystem
///
/// @param src          The ACE parts of the source acl
///
/// @param dest         The ACE parts of the destination acl
///
/// @return bool        return true if an acl was copied, false if not
///                     otherwise, there was an error in the copy process
///
fn copy_if_diff(
    dest_path: &Path,
    dest_ctx: &Smbc,
    src: (Sid, AceAtype, AceFlag, XAttrMask),
    dest: (Sid, AceAtype, AceFlag, XAttrMask),
) -> ForkliftResult<bool> {
    trace!("dtype {:?}, dflags {:?}, dmask {}", dest.1, dest.2, dest.3);
    trace!("atype {:?}, aflags {:?}, mask {}", src.1, src.2, src.3);
    if src.1 != dest.1 || src.2 != dest.2 || src.3 != dest.3 {
        let (new_acl, old_acl) = (
            ACE::Numeric(SidType::Numeric(Some(src.0)), src.1, src.2, src.3),
            ACE::Numeric(SidType::Numeric(Some(dest.0)), dest.1, dest.2, dest.3),
        );
        trace!("New {}, Old {}", new_acl, old_acl);
        replace_acl(dest_path, dest_ctx, &new_acl, &old_acl)?;
        return Ok(true);
    }
    Ok(false)
}

///
/// check if a source acl needs to be copied to the destination
/// if yes, copy the file, otherwise do nothing
///
/// @param dest_path    The destination filepath
///
/// @param dest_ctx     The Samba context of the destination filesystem
///
/// @param src          The ACE parts of the source acl
///
/// @param dest_acls    The list of destination acls
///
/// @return bool        return true if an acl was copied, false if not
///                     otherwise, there was an error in the copy process
///
fn copy_acl(
    dest_path: &Path,
    dest_ctx: &Smbc,
    src: (Sid, AceAtype, AceFlag, XAttrMask),
    dest_acls: &mut Vec<SmbcAclValue>,
) -> ForkliftResult<bool> {
    match check_acl_sid_remove(&src.0, dest_acls) {
        //check if same
        Some(ACE::Numeric(SidType::Numeric(Some(dest_sid)), dtype, dflags, dmask)) => {
            // if there any differences, copy
            copy_if_diff(dest_path, dest_ctx, src, (dest_sid, dtype, dflags, dmask))
        }
        //check if same here, if so, do nothing, else copy
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

///
/// map named acl Sids from a source file to their destinarion numeric Sids
/// then replace the incorrect destination acls with the source acls
///
/// @param dest_path        The destination filepath
///
/// @param dest_ctx         Samba context of destination
///
/// @param src_acls         numeric src acls for comparison
///
/// @param src_acls_plus    named arc acls for mapping
///
/// @param dest_acls        dest acls for mapping
///
/// @return                 true if an acl was copied or all dest acls were exhausted
///
pub fn map_names_and_copy(
    dest_path: &Path,
    dest_ctx: &Smbc,
    src_acls: &[SmbcAclValue],
    src_acls_plus: &[SmbcAclValue],
    dest_acls: &mut Vec<SmbcAclValue>,
) -> ForkliftResult<bool> {
    let mut map = match NAME_MAP.lock() {
        Ok(hm) => hm,
        Err(_) => {
            return Err(ForkliftError::FSError(
                "Could not get sid name map".to_string(),
            ));
        }
    };
    trace!(
        "ACLS:\nSRC: {:?}\n\nDEST {:?}\n",
        &src_acls_plus,
        &dest_acls
    );
    let (mut copied, mut count) = (false, 0);
    let mut creator_reached = false;
    for src_acl in src_acls_plus {
        match (src_acl.clone(), src_acls[count].clone()) {
            (
                SmbcAclValue::AclPlus(ACE::Named(SidType::Named(Some(sid)), _, _, _)),
                SmbcAclValue::Acl(ACE::Numeric(SidType::Numeric(Some(_)), atype, aflags, mask)),
            ) => {
                if sid == "\\Creator Owner" {
                    creator_reached = true;
                }
                //check if sid is mapped, add to map if not already in
                let mapped = match map.entry(sid.clone()) {
                    E::Occupied(o) => o.into_mut(),
                    E::Vacant(v) => v.insert(map_name(dest_path, dest_ctx, &sid)?),
                };
                trace!("Sid: {}, mapped: {}", sid, mapped);
                //if reached "CREATOR" sids, ignore the rest
                if !creator_reached {
                    copied = copy_acl(
                        dest_path,
                        dest_ctx,
                        (mapped.clone(), atype, aflags, mask),
                        dest_acls,
                    )?;
                }
            }
            (_, _) => {
                return Err(ForkliftError::FSError(
                    "input src acls are not formatted correctly!!".to_string(),
                ));
            }
        }
        count += 1;
    }
    for dest_acl in dest_acls.clone() {
        match dest_acl {
            SmbcAclValue::Acl(ACE::Numeric(SidType::Numeric(Some(dest_sid)), a, f, m)) => {
                //if not \\CREATOR Owner or Creator Group
                if !(dest_sid == Sid(vec![3, 0]) || dest_sid == Sid(vec![3, 1])) {
                    let ace = ACE::Numeric(SidType::Numeric(Some(dest_sid)), a, f, m);
                    match dest_ctx.removexattr(
                        dest_path,
                        &SmbcXAttr::AclAttr(SmbcAclAttr::Acl(ace.clone())),
                    ) {
                        Ok(_) => debug!("Removed extra acl {}", ace),
                        Err(e) => {
                            let err = format!(
                                "Error {}, failed to remove the old acl {} from {:?}",
                                e, ace, dest_path
                            );
                            error!("{}", err);
                            return Err(ForkliftError::FSError(err));
                        }
                    }
                }
            }
            _ => {
                return Err(ForkliftError::FSError(
                    "input dest acls are not formatted correctly!".to_string(),
                ));
            }
        }
    }
    Ok(copied || !dest_acls.is_empty())
}

///
/// get list of acl values
///
/// @param path the path of the file to grab the xattrs from
///
/// @param ctx  the context of the filesystem
///
/// @param plus boolean value denoting whether the returned list is named
///             or numeric
///
/// @return     return the list of acls of the input file
///
pub fn get_acl_list(path: &Path, fs: &Smbc, plus: bool) -> ForkliftResult<Vec<SmbcAclValue>> {
    let err = format!("unable to get acls from {:?}", path);
    let suc = "acl all get success";
    let mut acls = {
        if plus {
            let acl_plus_xattr = SmbcXAttr::AclAttr(SmbcAclAttr::AclAllPlus);
            get_xattr(path, fs, &acl_plus_xattr, &err, suc)?
        } else {
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
/// @param path     the path to the destination file
///
/// @param context  the filesystem context of the destination file
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
fn change_stat_mode(path: &Path, context: &NetworkContext, mode: u32) -> ForkliftResult<()> {
    match context.chmod(path, Mode::from_bits_truncate(mode)) {
        Ok(_) => {
            debug!("Chmod of file {:?} to {} ran", path, mode);
            Ok(())
        }
        Err(e) => {
            let err = format!("Error {}, mode {:?}", e, Mode::from_bits_truncate(mode));
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
/// @param dest         Dest file entry
///
/// @param src_context  the context of the source filesystem
///
/// @param dest_context the context of the destination filesystem
///
/// @return             the outcome of the permission rsync (or a ForkliftError if it fails)
///
pub fn copy_permissions(
    src: &Entry,
    dest: &Entry,
    src_context: &NetworkContext,
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
            match has_different_permissions(src, dest, src_context, dest_context) {
                Ok(true) => {
                    change_stat_mode(dest_path, dest_context, src_mode)?;
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
                dest_path,
                dest_ctx,
                &get_acl_list(src_path, src_ctx, false)?,
                &get_acl_list(src_path, src_ctx, true)?,
                &mut get_acl_list(dest_path, dest_ctx, false)?,
            )?;
            match has_different_permissions(src, dest, src_context, dest_context) {
                Ok(true) => {
                    trace!("src mode {}", src_mode);
                    change_stat_mode(dest_path, dest_context, src_mode)?;
                    change_mode(src_path, dest_path, src_ctx, dest_ctx)?;
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
