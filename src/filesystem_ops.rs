use ::smbc::*;
use chrono::NaiveDateTime;
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
use crate::postgres_logger::{send_mess, LogMessage};
use crate::progress_message::ProgressMessage;
use crate::tables::{current_time, ErrorType};

/// default buffer size
const BUFF_SIZE: u64 = 1024 * 1000;

lazy_static! {
    /// singleton containing a map of named ID's to Sid's
    pub static ref SID_NAME_MAP: Mutex<HashMap<String, Sid>> = {
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
    /// copied a file,  send path, src, dest checksum, size
    FileCopied(String, Vec<u8>, Vec<u8>, i64, NaiveDateTime),
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
    /// updated a file internal bytes, send path, src, dest checksum, size
    ChecksumUpdated(String, Vec<u8>, Vec<u8>, i64, NaiveDateTime),
}

/// checks if a path is valid
pub fn exist(path: &Path, context: &mut ProtocolContext) -> bool {
    context.stat(path).is_ok()
}

/// gets the relative path (the parts of the path in common) between base and comp
pub fn get_rel_path(base_path: &Path, comp_path: &Path) -> ForkliftResult<PathBuf> {
    match diff_paths(&base_path, &comp_path) {
        None => {
            let err =
                format!("Could not get relative path from {:?} to {:?}", &base_path, &comp_path);
            Err(ForkliftError::FSError(err))
        }
        Some(path) => Ok(path),
    }
}

/// set the external attribute of a destination file on a Samba server.
/// valid descriptors can be found in Smbc for the attribute.
/// The error and success strings are descriptions should the set either
/// fail or succeed.
///
/// @note           See Smbc.rs for notes on setxattr
pub fn set_xattr(
    path: &Path,
    context: &Smbc,
    attr: &SmbcXAttr,
    value: &SmbcXAttrValue,
    error: &str,
    success: &str,
) -> ForkliftResult<()> {
    if let Err(e) = context.setxattr(path, attr, value, XAttrFlags::SMBC_XATTR_FLAG_CREATE) {
        let err = format!("Error {}, {}", e, error);
        return Err(ForkliftError::FSError(err));
    }
    trace!("set success! {}", success);
    Ok(())
}

/// get the external attribute of a destination file on a Samba server
/// as a Vec<u8>.  valid descriptors can be found in Smbc for the attribute.
/// The error and success strings are descriptions should the set either
/// fail or succeed.
///
/// @note See Smbc.rs for notes on getxattr.  Please note that you can
/// in fact do an exclude for .* (all) operations. Also, all returned
/// vectors should end in the \{0}, or Null character.  If you want
/// to parse this using xattr_parser, you will have to pop off the null
/// terminator.
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
            Err(ForkliftError::FSError(err))
        }
    }
}

/// make and/or update a directory endpoint in the destination filesystem,
/// keeping Dos or Unix permissions (depending on if the context is Samba or NFS)
///
/// @note If the filesystem context is Samba CIFS, then please note that the
/// mode of the directory cannot go below 555 (see chmod notes in Smbc)
/// This function might print an error to the console on mkdir.  This is because
/// another thread may have already created the directory before mkdir could be executed.  
pub fn make_dir(
    src_path: &Path,
    dest_path: &Path,
    src_context: &mut ProtocolContext,
    dest_context: &mut ProtocolContext,
) -> ForkliftResult<SyncOutcome> {
    let outcome: SyncOutcome;
    let exists = exist(dest_path, dest_context);
    if !exists {
        if let Err(e) = dest_context.mkdir(dest_path) {
            let exists_now = exist(dest_path, dest_context);
            //Note, this might occur due to another thread creating the directory
            let err = format!(
                "Error {}, Could not create {:?}, check_exists: {:?}, current exists {:?}",
                e, dest_path, exists, exists_now
            );
            error!("{:?}", err); //this one does not need to be logged
            return Ok(SyncOutcome::UpToDate);
        }
    }
    let (src_entry, dest_entry) =
        (Entry::new(&src_path, src_context), Entry::new(&dest_path, dest_context));
    // make sure permissions match
    let copy_outcome = copy_permissions(&src_entry, &dest_entry, &src_context, &dest_context)?;
    debug!("Copy permissions successful");
    match (exists, copy_outcome) {
        (false, _) => outcome = SyncOutcome::DirectoryCreated,
        (_, SyncOutcome::PermissionsUpdated) => outcome = SyncOutcome::DirectoryUpdated,
        (..) => outcome = SyncOutcome::UpToDate,
    }
    Ok(outcome)
}
/// find any directories in the path that do not exist in the
/// filesystem and add them.
/// @note the src_path is used to ensure correctness in permissions,
/// while the root_file_path ensures correctness while looping over
/// parent directories.                   
pub fn make_dir_all(
    src_path: &Path,
    dest_path: &Path,
    root_file_path: &Path,
    src_context: &mut ProtocolContext,
    dest_context: &mut ProtocolContext,
) -> ForkliftResult<()> {
    let (mut stack, mut src_stack) = (vec![], vec![]);
    let (mut dest_parent, mut src_parent) = (dest_path.parent(), src_path.parent());
    trace!("dest parent {:?}", root_file_path);
    //add parent directory paths to stack
    while {
        match dest_parent {
            Some(parent) => parent != root_file_path,
            None => false,
        }
    } {
        trace!("dest_parent {:?} , root {:?}", dest_parent, root_file_path);
        match (dest_parent, src_parent) {
            (Some(destination_parent), Some(source_parent)) => {
                stack.push(destination_parent);
                src_stack.push(source_parent);
                dest_parent = destination_parent.parent();
                src_parent = source_parent.parent();
            }
            (..) => {
                return Err(ForkliftError::FSError(
                    "While loop invariant in make_dir_all failed".to_string(),
                ));
            }
        };
    }
    //check all directories in the path
    while !stack.is_empty() {
        trace!("stack not empty");
        let (path, srcpath) = match (stack.pop(), src_stack.pop()) {
            (Some(p), Some(sp)) => (p, sp),
            (..) => {
                return Err(ForkliftError::FSError("Loop invariant failed".to_string()));
            }
        };
        if !exist(&path, dest_context) {
            make_dir(srcpath, &path, src_context, dest_context)?;
            debug!("made dir {:?}", path);
        }
    }
    Ok(())
}

/// Check if the source and dest entries have the same size
/// Since size attributes remain the same for samba + nfs calls,
/// Can do comparison
pub fn has_different_size(src: &Entry, dest: &Entry) -> ForkliftResult<bool> {
    match (src.metadata(), dest.metadata()) {
        (None, _) => {
            let err = format!("File {:?} stat should not be None!", src.path());
            Err(ForkliftError::FSError(err))
        }
        (_, None) => Ok(true),
        (Some(src_stat), Some(dest_stat)) => Ok(src_stat.size() != dest_stat.size()),
    }
}

/// Check if the source entry is more recent than the dest entry
/// Since time attributes remain the same for samba + nfs calls,
/// we can do comparison.
/// @note this only checks mtime, since ctime are attr changes and we want
/// to know if there were any recent write changes
pub fn is_more_recent(src: &Entry, dest: &Entry) -> ForkliftResult<bool> {
    match (src.metadata(), dest.metadata()) {
        (Some(src_stat), Some(dest_stat)) => {
            Ok(src_stat.mtime().num_seconds() > dest_stat.mtime().num_seconds())
        }
        (None, _) => {
            let err = format!("Source File {:?} does not exist", src.path());
            Err(ForkliftError::FSError(err))
        }
        (_, None) => {
            trace!("Dest File does not exist");
            Ok(true)
        }
    }
}

/// this functions checks whether or not the destination file has the same
/// permission settings as the source file.
/// @note this function ONLY checks the mode attribute (DOS or LINUX) this function
/// DOES NOT check the external attributes (xattr)  of a file.  
pub fn has_different_permissions(
    src: &Entry,
    dest: &Entry,
    src_context: &ProtocolContext,
    dest_context: &ProtocolContext,
) -> ForkliftResult<bool> {
    //check file existence
    let (src_mode, dest_mode) = match (src.metadata(), dest.metadata()) {
        (None, _) => {
            return Err(ForkliftError::FSError("Source File does not exist".to_string()));
        }
        (_, None) => {
            debug!("Dest File does not exist");
            return Ok(true);
        }
        (Some(src_stat), Some(dest_stat)) => (src_stat.mode(), dest_stat.mode()),
    };

    match (src_context, dest_context) {
        (ProtocolContext::Nfs(_), ProtocolContext::Nfs(_)) => {
            trace!("src mode {:?}, dest mode {:?}", src_mode, dest_mode);
            Ok(src_mode != dest_mode)
        }
        (ProtocolContext::Samba(ctx), ProtocolContext::Samba(dctx)) => {
            let xattr = SmbcXAttr::DosAttr(SmbcDosAttr::Mode);
            let (err, suc) = ("get the dos mode failed", "dos mode retreived");
            let src_mod_values = get_xattr(src.path(), ctx, &xattr, err, suc)?;
            let dest_mod_values = get_xattr(dest.path(), dctx, &xattr, err, suc)?;
            trace!("src dos mode {:?}, dest dos mode {:?}", src_mod_values, dest_mod_values);
            Ok(src_mod_values != dest_mod_values)
        }
        (..) => Err(ForkliftError::FSError("Filesystems do not match!".to_string())),
    }
}

/// create an empty vector to store the name of a symlink
fn make_target(size: i64, readmax: u64) -> ForkliftResult<Vec<u8>> {
    let src_target: Vec<u8>;
    if size <= readmax as i64 {
        if size > 0 {
            src_target = vec![0; size as usize]
        } else {
            src_target = vec![0; readmax as usize]
        }
    } else {
        return Err(ForkliftError::FSError("File Name too long".to_string()));
    }
    Ok(src_target)
}

/// read a symlink into a String
fn read_link(path: &Path, nfs_context: &Nfs, size: i64) -> ForkliftResult<String> {
    let mut src_target: Vec<u8> = make_target(size, BUFF_SIZE)?;
    if let Err(e) = nfs_context.readlink(path, &mut src_target) {
        let err = format!("Unable to read link at {:?}, {:?}", path, e);
        return Err(ForkliftError::FSError(err));
    }
    let mut link = String::from_utf8(src_target)?;
    link.pop();
    Ok(link)
}

/// helper for copy_link; unlink a symlink in order to update it
pub fn unlink_outdated_link(path: &Path, nfs_context: &Nfs) -> ForkliftResult<()> {
    if let Err(e) = nfs_context.unlink(path) {
        return Err(ForkliftError::FSError(format!(
            "Could not remove {:?} while updating link, {}",
            path, e
        )));
    }
    Ok(())
}

/// check if the destination symlink links to the same file as the source, copy if not
///
/// @note Samba does not support symlinks, so copy_link will immediately return
///  with an error
pub fn copy_link(
    src: &Entry,
    dest: &Entry,
    src_context: &mut ProtocolContext,
    dest_context: &mut ProtocolContext,
    log_output: &Sender<LogMessage>,
) -> ForkliftResult<SyncOutcome> {
    //Check if correct Filesytem
    let (src_nfs_context, dest_nfs_context) = match (src_context.clone(), dest_context.clone()) {
        (ProtocolContext::Nfs(ctx), ProtocolContext::Nfs(dctx)) => (ctx, dctx),
        (..) => {
            return Err(ForkliftError::FSError("Samba does not support symlinks".to_string()));
        }
    };
    //Check if files exist and get size
    let (size, dest_size) = match (src.metadata(), dest.metadata()) {
        (None, _) => {
            return Err(ForkliftError::FSError("Source File does not exist!".to_string()));
        }
        (Some(src_stat), None) => (src_stat.size() + 1, 0),
        (Some(src_stat), Some(dest_stat)) => (src_stat.size() + 1, dest_stat.size() + 1),
    };
    let (src_path, dest_path) = (src.path(), dest.path());
    let src_target = read_link(src_path, &src_nfs_context, size)?;
    let mut outcome: SyncOutcome;
    match dest.is_link() {
        Some(true) => {
            let dest_target = read_link(dest_path, &dest_nfs_context, dest_size)?;
            if dest_target != src_target {
                unlink_outdated_link(dest_path, &dest_nfs_context)?;
                outcome = SyncOutcome::SymlinkUpdated;
            } else {
                return Ok(SyncOutcome::UpToDate);
            }
        }
        Some(false) => {
            //Not safe to delete
            return Err(ForkliftError::FSError(format!(
                "Refusing to replace existing path {:?} by symlink",
                dest_path
            )));
        }
        None => {
            outcome = SyncOutcome::SymlinkCreated;
        }
    }
    //create/update symlink, if creation fails, skip
    if let Err(e) = dest_nfs_context.symlink(Path::new(&src_target), dest_path) {
        let mess = LogMessage::ErrorType(
            ErrorType::FSError,
            format!("Error {}, could not create link from {:?} to {:?}", e, dest_path, src_target),
        );
        send_mess(mess, log_output)?;
        outcome = SyncOutcome::SymlinkSkipped;
    }
    Ok(outcome)
}

/// read as much data from a file from the offset as possible in one pass
///
/// @note           while this function will attempt to read BUFF_SIZE
///                 bytes from the file starting from offset, it is
///                 still possible that the actual number of bytes read is
///                 smaller.  Be sure to check the length of the returned
///                 vector for the true number of bytes read
fn read_chunk(path: &Path, file: &FileType, offset: u64) -> ForkliftResult<Vec<u8>> {
    match file.read(BUFF_SIZE, offset) {
        Ok(buf) => Ok(buf),
        Err(e) => {
            let err = format!("Error {:?}, Could not read from {:?}", e, path,);
            Err(ForkliftError::FSError(err))
        }
    }
}

/// open the file at path in context.
///
/// @note since mode is never used, we can set mode to be anything
fn open_file(
    path: &Path,
    context: &mut ProtocolContext,
    flags: OFlag,
    error: &str,
) -> ForkliftResult<FileType> {
    match context.open(path, flags, Mode::S_IRWXU | Mode::S_IRWXG | Mode::S_IRWXO) {
        Ok(f) => Ok(f),
        Err(e) => {
            let err = format!("Error {}, {}", e, error);
            Err(ForkliftError::FSError(err))
        }
    }
}

/// write buffer to the file at path starting at the offset
///
/// @note           sometimes the size of the input buffer is too
///                 big to write, so we can check the num written
///                 against the size of the buffer to see if we
///                 need to write again
fn write_file(path: &Path, file: &FileType, buffer: &[u8], offset: u64) -> ForkliftResult<u64> {
    match file.write(buffer, offset) {
        Ok(n) => Ok(n),
        Err(e) => {
            let err = format!("Error {}, Could not write to {:?}", e, path);
            Err(ForkliftError::FSError(err))
        }
    }
}
/// helper for checksum copy; creates a new file at path
fn file_create(path: &Path, context: &mut ProtocolContext, err: &str) -> ForkliftResult<FileType> {
    match context.create(&path, OFlag::O_CREAT, Mode::S_IRWXU | Mode::S_IRWXO | Mode::S_IRWXG) {
        Ok(f) => Ok(f),
        Err(e) => Err(ForkliftError::FSError(format!("Error {}, {}", e, err))),
    }
}

/// helper for checksum copy; hash a buffer of bytes to a checksum vec of bytes
fn hash(buf: &[u8], hasher: &mut MeowHasher) -> Vec<u8> {
    hasher.input(buf);
    hasher.result_reset().as_slice().to_vec()
}

/// helper for checksum copy; trucate a buffer to the number written and append to total
fn update_buffer(buf: &mut Vec<u8>, total_buf: &mut Vec<u8>, num_written: u64) {
    buf.truncate(num_written as usize);
    total_buf.append(buf);
}
/// Send progress and log errors
fn send_progress(
    progress: ProgressMessage,
    progress_output: &Sender<ProgressMessage>,
    log_output: &Sender<LogMessage>,
) -> ForkliftResult<()> {
    if progress_output.send(progress).is_err() {
        let mess = LogMessage::ErrorType(
            ErrorType::CrossbeamChannelError,
            "Unable to send progress".to_string(),
        );
        send_mess(mess, log_output)?;
    }
    Ok(())
}
/// checksums a destination file, copying over the data from the src file in the chunks
/// where checksum fails. is_copy is used to determine if file copy or checksum copy
pub fn checksum_copy(
    src: &Entry,
    dest: &Entry,
    src_context: &mut ProtocolContext,
    dest_context: &mut ProtocolContext,
    is_copy: bool,
    progress_output: &Sender<ProgressMessage>,
    log_output: &Sender<LogMessage>,
) -> ForkliftResult<SyncOutcome> {
    let (src_path, dest_path) = (src.path(), dest.path());
    let size = match src.metadata() {
        Some(m) => m.size(),
        None => {
            return Err(ForkliftError::FSError(format!("Source file {:?} should exist!", src_path)));
        }
    };
    // open src and dest files
    let src_err = format!("Could not open {:?} for reading", src_path);
    let dest_err = format!("could not open {:?} for writing", dest_path);
    let src_file = open_file(src_path, src_context, OFlag::empty(), &src_err)?;
    let dest_file = if is_copy {
        file_create(&dest_path, dest_context, &dest_err)?
    } else {
        open_file(dest_path, dest_context, OFlag::O_CREAT, &dest_err)?
    };
    //loop until end, count the number of times we needed to update the file
    let (mut src_total, mut dest_total): (Vec<u8>, Vec<u8>) = (vec![], vec![]);
    let (mut offset, mut counter) = (0, 0);
    let mut hasher = MeowHasher::new();
    let path = src_path.to_string_lossy().to_string();
    loop {
        let mut num_written = 0;
        let mut src_buf = read_chunk(src_path, &src_file, offset)?;
        let mut dest_buf = read_chunk(dest_path, &dest_file, offset)?;
        let (hash_src, hash_dest) = (hash(&src_buf, &mut hasher), hash(&dest_buf, &mut hasher));
        if hash_src != hash_dest {
            if src_buf.len() < dest_buf.len() {
                dest_file.truncate(src_buf.len() as u64)?;
            }
            //write src_buf -> dest
            num_written = write_file(dest_path, &dest_file, &src_buf, offset)?;
            counter += 1;
            dest_buf = read_chunk(dest_path, &dest_file, offset)?;
        }
        //update offset, add bytes to file_bufs and check if num_written > 0
        if num_written > 0 {
            update_buffer(&mut src_buf, &mut src_total, num_written);
            update_buffer(&mut dest_buf, &mut dest_total, num_written);
            offset += num_written - 1;
        } else {
            update_buffer(&mut src_buf, &mut src_total, num_written);
            offset += src_buf.len() as u64;
        }
        if src_buf.is_empty() {
            break;
        }
        //send progress
        let progress = ProgressMessage::CheckSyncing {
            description: path.clone(),
            size: size as usize,
            done: offset as usize,
        };
        send_progress(progress, progress_output, log_output)?;
    }
    let (src_check, dest_check) = (hash(&src_total, &mut hasher), hash(&dest_total, &mut hasher));
    if src_check != dest_check {
        return Err(ForkliftError::ChecksumError(format!(
            "{} has source checksum {:?}, destination checksum {:?}",
            path, src_check, dest_check
        )));
    }
    if counter == 0 {
        return Ok(SyncOutcome::UpToDate);
    }
    if is_copy {
        Ok(SyncOutcome::FileCopied(path, src_check, dest_check, size, current_time()))
    } else {
        Ok(SyncOutcome::ChecksumUpdated(path, src_check, dest_check, size, current_time()))
    }
}

/// syncs the src and dest files.  It also sends the current progress
/// of the rsync of the entry.
///
/// @note                   no destination => FileCopied, diff size => File Copied
///                         diff srctime more recent => File Copied
///                         Otherwise, checksum copy
pub fn sync_entry(
    src: &Entry,
    dest: &Entry,
    src_context: &mut ProtocolContext,
    dest_context: &mut ProtocolContext,
    progress_output: &Sender<ProgressMessage>,
    log_output: &Sender<LogMessage>,
) -> ForkliftResult<SyncOutcome> {
    let description = src.path().to_string_lossy().into_owned();
    send_progress(ProgressMessage::StartSync(description), progress_output, log_output)?;
    match src.is_link() {
        Some(true) => {
            trace!("Is link!");
            return copy_link(src, dest, src_context, dest_context, log_output);
        }
        Some(false) => (),
        None => {
            let err = format!("Source file {:?} is_link should not be None!", src.path());
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
            return Err(ForkliftError::FSError(err));
        }
    }
    //check if size different or if src more recent than dest
    match (dest.metadata(), has_different_size(src, dest), is_more_recent(src, dest)) {
        (None, _, _) => {
            debug!("Destination does not exist yet!");
            checksum_copy(src, dest, src_context, dest_context, true, progress_output, log_output)
        }
        (Some(_), Ok(size_dif), Ok(recent)) => {
            debug!("Is different!!! size {}  recent {}", size_dif, recent);
            if size_dif || recent {
                checksum_copy(
                    src,
                    dest,
                    src_context,
                    dest_context,
                    true,
                    progress_output,
                    log_output,
                )
            } else {
                checksum_copy(
                    src,
                    dest,
                    src_context,
                    dest_context,
                    false,
                    progress_output,
                    log_output,
                )
            }
        }
        (_, Err(e), _) => Err(e),
        (_, _, Err(e)) => Err(e),
    }
}

/// given a source sid, check through a list of destination acls for the acl
/// with the matching destination sid, remove it from the list and return it
///
/// @return             return Some<ACE>, where ACE is the acl in the list
///                     of destination acls with the same sid as check_sid
///                     otherwise if dest_acls does not have an acl with the
///                     input sid
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
                let sid = SidType::Numeric(Some(dest_sid.clone()));
                let return_ace = ACE::Numeric(sid, atype.clone(), *flag, *mask);
                dest_acls.remove(count);
                return Some(return_ace);
            }
        }
    }
    None
}

/// Change the Dos Mode Attribute of the destination file to match
/// the source Dos Mode Attribute
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
            let err = format!("Error {}, unable to parse xattr from file {:?}", e, src_path);
            return Err(ForkliftError::FSError(err));
        }
    };
    trace!("dosmode src is {:?}", &dosmode);
    let err = format!("unable to set dos mode {} for file {:?}", &dosmode, dest_path);
    let suc = format!("set file {:?} dosmode to {}", dest_path, &dosmode);

    set_xattr(dest_path, dest_ctx, &mode_xattr, &dosmode, &err, &suc)?;
    Ok(())
}

/// get the numeric destination SID corresponding to a named
/// source SID.
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

/// Temporarily map a named acl from the source filesystem to a file at dest_path
/// in order to determine the numeric equivalent of the mapped sid in the destination
/// file, then remove the temporary acl
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
    let return_sid = match get_mapped_sid(dest_path, dest_ctx, &sid, &dest_acls) {
        Ok(Some(dest_sid)) => dest_sid,
        Ok(None) => {
            let err = format!("Unsucessful in mapping sid {}", &sid);
            return Err(ForkliftError::FSError(err));
        }
        Err(e) => return Err(e),
    };
    if let Err(e) =
        dest_ctx.removexattr(dest_path, &SmbcXAttr::AclAttr(SmbcAclAttr::AclPlus(temp_ace)))
    {
        let err = format!("Error {}, failed to remove temp acl", e);
        return Err(ForkliftError::FSError(err));
    }
    debug!("removing temp acl success");
    Ok(return_sid)
}

/// remove the old acl from the destiation file and set a new one to replace
/// it with a new acl
fn replace_acl(
    dest_path: &Path,
    dest_ctx: &Smbc,
    new_acl: &ACE,
    old_acl: &ACE,
) -> ForkliftResult<()> {
    //remove the old acl first
    if let Err(e) =
        dest_ctx.removexattr(dest_path, &SmbcXAttr::AclAttr(SmbcAclAttr::Acl(old_acl.clone())))
    {
        let err =
            format!("Error {}, failed to remove the old acl {} from {:?}", e, old_acl, dest_path);
        return Err(ForkliftError::FSError(err));
    };
    trace!("Removed old acl {}", old_acl);
    //set new acl
    let xattr = SmbcXAttr::AclAttr(SmbcAclAttr::AclNone);
    let val = SmbcXAttrValue::Ace(new_acl.clone());
    let err = format!("failed to set the new acl {} path {:?}", new_acl, dest_path);
    let suc = format!("Set new acl {}", new_acl);
    set_xattr(dest_path, dest_ctx, &xattr, &val, &err, &suc)?;
    Ok(())
}

/// map the named source sid to a numeric destination sid
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

/// Copy the acl from source to destination if they are different
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

/// check if a source acl needs to be copied to the destination
/// if yes, copy the file, otherwise do nothing
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
            let (err, suc) = ("failed to copy new acl", "Copied new acl");
            set_xattr(dest_path, dest_ctx, &xattr_set, &val, err, suc)?;
            Ok(true)
        }
    }
}

/// map named acl Sids from a source file to their destinarion numeric Sids
/// then replace the incorrect destination acls with the source acls
pub fn map_names_and_copy(
    dest_path: &Path,
    dest_ctx: &Smbc,
    src_acls: &[SmbcAclValue],
    src_acls_plus: &[SmbcAclValue],
    dest_acls: &mut Vec<SmbcAclValue>,
) -> ForkliftResult<bool> {
    let mut map = match SID_NAME_MAP.lock() {
        Ok(hm) => hm,
        Err(_) => {
            return Err(ForkliftError::FSError("Could not get sid name map".to_string()));
        }
    };
    trace!("ACLS:\nSRC: {:?}\n\nDEST {:?}\n", &src_acls_plus, &dest_acls);
    let (mut copied, mut creator_reached, mut count) = (false, false, 0);
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
                let temp_ace = (mapped.clone(), atype, aflags, mask);
                if !creator_reached {
                    copied = copy_acl(dest_path, dest_ctx, temp_ace, dest_acls)?;
                }
            }
            (..) => {
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
                    if let Err(e) = dest_ctx
                        .removexattr(dest_path, &SmbcXAttr::AclAttr(SmbcAclAttr::Acl(ace.clone())))
                    {
                        let err = format!(
                            "Error {}, failed to remove the old acl {} from {:?}",
                            e, ace, dest_path
                        );
                        return Err(ForkliftError::FSError(err));
                    }
                    debug!("Removed extra acl {}", ace);
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

/// get list of acl values
/// plus denotes whether the returned list is named or numeric
pub fn get_acl_list(
    path: &Path,
    smb_context: &Smbc,
    plus: bool,
) -> ForkliftResult<Vec<SmbcAclValue>> {
    let err = format!("unable to get acls from {:?}", path);
    let suc = "acl all get success";
    let mut acls = if plus {
        let acl_plus_xattr = SmbcXAttr::AclAttr(SmbcAclAttr::AclAllPlus);
        get_xattr(path, smb_context, &acl_plus_xattr, &err, suc)?
    } else {
        let acl_xattr = SmbcXAttr::AclAttr(SmbcAclAttr::AclAll);
        get_xattr(path, smb_context, &acl_xattr, &err, &suc)?
    };
    acls.pop();
    let acl_list = match xattr_parser(CompleteByteSlice(&acls)) {
        Ok((_, acl_list)) => acl_list,
        Err(e) => {
            let err =
                format!("Error {}, unable to parse acls {}", e, String::from_utf8_lossy(&acls));
            return Err(ForkliftError::FSError(err));
        }
    };
    match acl_list {
        SmbcXAttrValue::AclAll(s_acls) => Ok(s_acls),
        _ => Ok(vec![]),
    }
}

/// change the destination linux stat mode to that of the source file
/// @note           In a Samba Context, chmod will change the Dos Mode
///                 (it's necessary for Normal Dos Mode, otherwise
///                 a normal Dos Mode will be treated as an Archive file)
///                 However, it may not change the linux stat mode
///                 correctly (depends on your config file, see Smbc for
///                 details)
fn change_stat_mode(path: &Path, context: &ProtocolContext, mode: u32) -> ForkliftResult<()> {
    if let Err(e) = context.chmod(path, Mode::from_bits_truncate(mode)) {
        let err = format!("Error {}, mode {:?}", e, Mode::from_bits_truncate(mode));
        return Err(ForkliftError::FSError(err));
    }
    debug!("Chmod of file {:?} to {} ran", path, mode);
    Ok(())
}

/// Copy the source permissions to the destination file and return the
/// current status of the rsync (specifically of the permissions)
pub fn copy_permissions(
    src: &Entry,
    dest: &Entry,
    src_context: &ProtocolContext,
    dest_context: &ProtocolContext,
) -> ForkliftResult<SyncOutcome> {
    let src_mode = match (src.is_link(), src.metadata()) {
        (Some(false), Some(stat)) => stat.mode(),
        (Some(true), _) => return Ok(SyncOutcome::UpToDate),
        (..) => {
            return Err(ForkliftError::FSError(format!("src {:?} does not exist", src.path())));
        }
    };
    let (src_path, dest_path) = (src.path(), dest.path());
    let outcome;
    match (src_context, dest_context) {
        //stat mode diff
        (ProtocolContext::Nfs(_), ProtocolContext::Nfs(_)) => {
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
        (ProtocolContext::Samba(src_ctx), ProtocolContext::Samba(dest_ctx)) => {
            let src_acl = &get_acl_list(src_path, src_ctx, false)?;
            let src_plus_acl = &get_acl_list(src_path, src_ctx, true)?;
            let dest_acl = &mut get_acl_list(dest_path, dest_ctx, false)?;
            let copied = map_names_and_copy(dest_path, dest_ctx, src_acl, src_plus_acl, dest_acl)?;
            match has_different_permissions(src, dest, src_context, dest_context) {
                Ok(true) => {
                    trace!("src mode {}", src_mode);
                    change_stat_mode(dest_path, dest_context, src_mode)?;
                    change_mode(src_path, dest_path, src_ctx, dest_ctx)?;
                    outcome = SyncOutcome::PermissionsUpdated
                }
                Ok(false) => {
                    outcome = if copied {
                        SyncOutcome::PermissionsUpdated
                    } else {
                        SyncOutcome::UpToDate
                    };
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        (..) => return Err(ForkliftError::FSError("Different contexts!".to_string())),
    }
    Ok(outcome)
}
