extern crate digest;
extern crate meowhash;
extern crate nix;
extern crate nom;
extern crate smbc;

use self::digest::Digest;
use self::meowhash::*;
use self::nix::fcntl::OFlag;
use self::nix::sys::stat::{Mode, SFlag};
use self::nom::types::CompleteByteSlice;

use error::{ForkliftError, ForkliftResult};
use filesystem::*;
use smbc::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const BUFF_SIZE: u64 = 1024 * 1000;

lazy_static! {
    static ref NAME_MAP: Mutex<HashMap<String, Sid>> = Mutex::new(HashMap::new());
}

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
pub fn make_dir_all(p: &Path, fs: &mut NetworkContext) -> ForkliftResult<()> {
    let mut stack = vec![];
    let mut q = p.parent();
    while { q != None } {
        stack.push(q.unwrap()); //Note, can do b/c loop invariant (q must be Some(t))
        q = p.parent();
    }

    while !stack.is_empty() {
        let path = stack.pop().unwrap(); //Poss b/c loop invariant (stack not empty)

        if !exist(&path, fs) {
            match fs.mkdir(path) {
                Ok(_) => (),
                Err(e) => {
                    let err = format!("Error {}, Could not create {}", e, path.to_string_lossy());
                    return Err(ForkliftError::FSError(err));
                }
            };
        }
    }
    Ok(())
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
                    println!(
                        "src mode {:?}, dest mode {:?}",
                        src_meta.mode(),
                        dest_meta.mode()
                    );
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
                    let src_mod_values = ctx
                        .getxattr(src.path().as_path(), &SmbcXAttr::DosAttr(SmbcDosAttr::Mode))
                        .unwrap();
                    let dest_mod_values = dctx
                        .getxattr(
                            dest.path().as_path(),
                            &SmbcXAttr::DosAttr(SmbcDosAttr::Mode),
                        ).unwrap();
                    println!(
                        "src dos mode {:?}, dest dos mode {:?}",
                        src_mod_values, dest_mod_values
                    );
                    src_mod_values != dest_mod_values
                }
            }
        }
    }
}

pub fn has_different_acls(
    src: &Entry,
    src_context: &NetworkContext,
    dest: &Entry,
    dest_context: &NetworkContext,
) -> bool {
    //check file existence
    match dest.metadata() {
        Some(_) => (),
        None => {
            trace!("Dest File does not exist");
            return true;
        }
    };

    match src.metadata() {
        Some(_) => (),
        None => {
            error!("Source File does not Exist");
            panic!("Source File does not exist")
        }
    };

    match src_context {
        NetworkContext::Nfs(_) => {
            match dest_context {
                //shouldn't be nfs, but they should always have same acls
                NetworkContext::Nfs(_) => return false,
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
                    let mut dest_all = dctx
                        .getxattr(dest.path().as_path(), &SmbcXAttr::AllPlus)
                        .unwrap();
                    println!("all acls{}", String::from_utf8_lossy(&dest_all));
                    let (src_acls, dest_acls) =
                        get_acl_lists(ctx, dctx, src.path(), dest.path(), true, true);
                    src_acls != dest_acls
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
            offset += (num_written - 1) as u64;
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

//don't forget to add a progress sender at some point......
pub fn sync_entry(
    src: &Entry,
    src_context: &mut NetworkContext,
    dest: &Entry,
    dest_context: &mut NetworkContext,
) -> ForkliftResult<SyncOutcome> {
    match src.is_link() {
        Some(true) => {
            println!("Is link!");
            return copy_link(src, src_context, dest, dest_context);
        }
        Some(false) => (),
        None => panic!("src.is_link should not be None!!!"),
    }

    let diff_size = has_different_size(src, dest);
    let more_recent = is_more_recent(src, dest);
    //if files diff
    if more_recent || diff_size {
        println!("Is different!!! size {}  recent {}", diff_size, more_recent);
        return copy_entry(src, src_context, dest, dest_context);
    } else {
        //do a checksum to double check...
        return checksum_copy(src, src_context, dest, dest_context);
    }
}

fn check_acl_sid(src_sid: Sid, dest_vec: &Vec<SmbcAclValue>) -> Option<ACE> {
    for a in dest_vec {
        match a {
            SmbcAclValue::Acl(ACE::Numeric(SidType::Numeric(Some(sid)), atype, aflag, mask)) => {
                if src_sid == *sid {
                    return Some(ACE::Numeric(
                        SidType::Numeric(Some(sid.clone())),
                        atype.clone(),
                        *aflag,
                        *mask,
                    ));
                }
            }
            _ => (),
        }
    }
    None
}

fn check_acl_sid_remove(src_sid: String, dest_vec: &mut Vec<SmbcAclValue>) -> Option<ACE> {
    let map = NAME_MAP.lock().unwrap();
    let src_sid = map.get(&src_sid).unwrap();
    let mut count = 0;
    for a in dest_vec.clone() {
        match a {
            SmbcAclValue::Acl(ACE::Numeric(SidType::Numeric(Some(sid)), atype, aflag, mask)) => {
                if *src_sid == sid {
                    &dest_vec.remove(count);
                    return Some(ACE::Numeric(
                        SidType::Numeric(Some(sid)),
                        atype,
                        aflag,
                        mask,
                    ));
                }
            }
            _ => (),
        }
        count += 1;
    }
    None
}

fn change_mode(src_ctx: &Smbc, dest_ctx: &Smbc, src_path: &PathBuf, dest_path: &PathBuf) {
    let mut m = src_ctx
        .getxattr(src_path, &SmbcXAttr::DosAttr(SmbcDosAttr::Mode))
        .unwrap();
    m.pop();
    println!("attribute sans last....{}", String::from_utf8_lossy(&m));
    let dosmode = xattr_parser(CompleteByteSlice(&m)).unwrap().1;
    println!("dosmode src is {:?}", &dosmode);
    match dest_ctx.setxattr(
        dest_path,
        &SmbcXAttr::DosAttr(SmbcDosAttr::Mode),
        &dosmode,
        XAttrFlags::SMBC_XATTR_FLAG_CREATE,
    ) {
        Ok(_) => println!("setxattr ran"),
        Err(e) => println!("setxattr failed {}", e),
    }
}

/*
    get the numeric destination SID corresponding to the named 
    source SID.

    @param sid  The named source SID

    @param dest_acls_plus A vector of all of the destination acls named

    @param dest_ctx The filesystem context of the destination filesystem

    @param dest_path The path of the file whose acl's you are checking

    @return Option<Sid>  Some(SID) if a matching SID was found, NONE if 
            the file does not have a named equivalent ACL.
*/
fn get_mapped_sid(
    sid: &String,
    dest_acls_plus: Vec<SmbcAclValue>,
    dest_ctx: &Smbc,
    dest_path: &PathBuf,
) -> Option<Sid> {
    //let dest_acls = get_acl_list(dest_ctx, dest_path, false);
    let mut count = 0;
    for ace in dest_acls_plus {
        match ace {
            SmbcAclValue::AclPlus(ACE::Named(SidType::Named(Some(dest_sid)), _, _, _)) => {
                println!("src sid {} dest_sid {}", &sid, &dest_sid);
                if *sid == dest_sid {
                    println!("equals src sid {} dest_sid {}", &sid, &dest_sid);
                    let dest_acls = get_acl_list(dest_ctx, dest_path, false);
                    let send_ace = &dest_acls[count];
                    match send_ace {
                        SmbcAclValue::Acl(ACE::Numeric(
                            SidType::Numeric(Some(send_dest_sid)),
                            _,
                            _,
                            _,
                        )) => {
                            println!("{:?}", send_dest_sid);
                            return Some(send_dest_sid.clone());
                        }
                        _ => (),
                    }
                }
            }
            _ => (),
        }
        count += 1;
    }
    None
}

/*
    map named acl Sids from a src file to their destination sids
 */
pub fn map_names(src_acls: Vec<SmbcAclValue>, dest_ctx: &Smbc, dest_path: &PathBuf) {
    let mut map = NAME_MAP.lock().unwrap();
    for a in src_acls {
        let mut added_acl = false;
        match a {
            SmbcAclValue::AclPlus(ACE::Named(SidType::Named(Some(sid)), atype, aflags, mask)) => {
                match map.get(&sid) {
                    Some(_) => (),
                    None => {
                        let destplus = get_acl_list(dest_ctx, dest_path, true);
                        match get_mapped_sid(&sid, destplus, dest_ctx, dest_path) {
                            //add the acl to the file and call get_mapped_sid again
                            None => {
                                let temp_ace = ACE::Named(
                                    SidType::Named(Some(sid.clone())),
                                    AceAtype::ALLOWED,
                                    AceFlag::NONE,
                                    "FULL".to_string(),
                                );
                                match dest_ctx.setxattr(
                                    dest_path,
                                    &SmbcXAttr::AclAttr(SmbcAclAttr::AclNonePlus),
                                    &SmbcXAttrValue::Ace(temp_ace.clone()),
                                    XAttrFlags::SMBC_XATTR_FLAG_CREATE,
                                ) {
                                    Ok(_) => println!("Set a temp acl success"),
                                    Err(e) => println!("Set a temp acl fail: {}", e),
                                };
                                match get_mapped_sid(
                                    &sid,
                                    get_acl_list(dest_ctx, dest_path, true),
                                    dest_ctx,
                                    dest_path,
                                ) {
                                    Some(dest_sid) => {
                                        map.insert(sid, dest_sid);
                                    }
                                    //ERROR!
                                    None => panic!("unsuccessful in mapping sid {}", &sid),
                                };
                                match dest_ctx.removexattr(
                                    dest_path,
                                    &SmbcXAttr::AclAttr(SmbcAclAttr::Acl(temp_ace)),
                                ) {
                                    Ok(_) => println!("removing temp acl success"),
                                    Err(e) => println!("removing temp acl fail {}", e),
                                }
                            }
                            //the acl is in, map a to the returned sid
                            Some(dest_sid) => {
                                map.insert(sid, dest_sid);
                            }
                        };
                    }
                }
            }
            _ => (),
        }
    }
    println!("{:?}", *map);
}

/*
    get list of acl values
    @param ctx  the context of the filesystem

    @param path the path of the file to grab the xattrs from

    @param plus boolean value denoting whether the returned list is named
                or numeric
    
    @return     return the list of acls of the input file
*/
pub fn get_acl_list(ctx: &Smbc, path: &PathBuf, plus: bool) -> Vec<SmbcAclValue> {
    let mut acls = match plus {
        true => ctx
            .getxattr(path, &SmbcXAttr::AclAttr(SmbcAclAttr::AclAllPlus))
            .unwrap(),
        false => ctx
            .getxattr(path, &SmbcXAttr::AclAttr(SmbcAclAttr::AclAll))
            .unwrap(),
    };
    acls.pop();
    let acl_list = xattr_parser(CompleteByteSlice(&acls)).unwrap().1;
    match acl_list {
        SmbcXAttrValue::AclAll(s_acls) => s_acls,
        _ => vec![],
    }
}

/*
    returns the src and dest acl lists at once
 */
fn get_acl_lists(
    src_ctx: &Smbc,
    dest_ctx: &Smbc,
    src_path: &PathBuf,
    dest_path: &PathBuf,
    src_plus: bool,
    dest_plus: bool,
) -> (Vec<SmbcAclValue>, Vec<SmbcAclValue>) {
    let dlist = get_acl_list(dest_ctx, dest_path, dest_plus);
    let slist = get_acl_list(src_ctx, src_path, src_plus);
    (slist, dlist)
}

pub fn copy_permissions(
    src: &Entry,
    src_context: &NetworkContext,
    dest: &Entry,
    dest_context: &NetworkContext,
) -> ForkliftResult<()> {
    match src.is_link() {
        Some(true) => return Ok(()),
        Some(false) => (),
        None => panic!("is_link was None for {:?}", src.path()),
    }

    let src_meta = match src.metadata() {
        Some(stat) => stat,
        None => panic!("src_meta was None for {:?}", src.path()),
    };

    let dest_path = dest.path();
    let src_path = src.path();

    match dest_context {
        //stat mode diff
        NetworkContext::Nfs(_) => {
            if has_different_permissions(src, src_context, dest, dest_context) {
                dest_context
                    .chmod(dest_path, Mode::from_bits_truncate(src_meta.mode()))
                    .unwrap();
            }
        }
        //dos mod diff
        NetworkContext::Samba(dest_ctx) => match src_context {
            NetworkContext::Nfs(_) => panic!("Different contexts!"),
            NetworkContext::Samba(src_ctx) => {
                if has_different_permissions(src, src_context, dest, dest_context) {
                    change_mode(src_ctx, dest_ctx, src_path, dest_path);
                }
                println!("Made it...");
                if has_different_acls(src, src_context, dest, dest_context) {
                    /*let (slist, mut dlist) = get_acl_lists(src_ctx, dest_ctx, src_path, dest_path);
                    for a in slist {
                        match a {
                            SmbcAclValue::Acl(ACE::Numeric(
                                SidType::Numeric(Some(sid)),
                                atype,
                                aflag,
                                mask,
                            )) => {
                                let ace_exists = check_acl_sid_remove(sid.clone(), &mut dlist);
                                let ace =
                                    ACE::Numeric(SidType::Numeric(Some(sid)), atype, aflag, mask);
                                match ace_exists {
                                    None => match dest_ctx.setxattr(
                                        dest_path,
                                        &SmbcXAttr::AclAttr(SmbcAclAttr::AclNone),
                                        &SmbcXAttrValue::Ace(ace),
                                        XAttrFlags::SMBC_XATTR_FLAG_CREATE,
                                    ) {
                                        Ok(_) => println!("Setxattr ran"),
                                        Err(e) => println!("Setxattr failed {}", e),
                                    },
                                    Some(d_ace) => {
                                        if ace != d_ace {
                                            match dest_ctx.removexattr(
                                                dest_path,
                                                &SmbcXAttr::AclAttr(SmbcAclAttr::Acl(
                                                    d_ace.clone(),
                                                )),
                                            ) {
                                                Ok(_) => println!("Remove ran"),
                                                Err(e) => println!("Remove failed {}", e),
                                            }
                                            match dest_ctx.setxattr(
                                                dest_path,
                                                &SmbcXAttr::AclAttr(SmbcAclAttr::AclNone),
                                                &SmbcXAttrValue::Ace(ace),
                                                XAttrFlags::SMBC_XATTR_FLAG_CREATE,
                                            ) {
                                                Ok(_) => println!("Setxattr ran"),
                                                Err(e) => println!("Setxattr failed {}", e),
                                            }
                                        }
                                    }
                                }
                            }
                            _ => println!("Improperly parsed an acl!!!!"),
                        }
                    }
                    //if dlist is non-empty, then we need to remove the extra xattr
                    if !dlist.is_empty() {
                        for dace in dlist {
                            match dace {
                                SmbcAclValue::Acl(ace) => match dest_ctx.removexattr(
                                    dest_path,
                                    &SmbcXAttr::AclAttr(SmbcAclAttr::Acl(ace.clone())),
                                ) {
                                    Ok(_) => println!("Remove ran on {:?}", ace),
                                    Err(e) => println!("Remove failed {}", e),
                                },
                                _ => println!("Improperly parsed an acl!!!!"),
                            }
                        }
                    }*/
                }
            }
        },
    }

    Ok(())
}
