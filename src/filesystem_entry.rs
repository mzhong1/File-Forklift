use crate::filesystem::*;
use log::*;
use nix::sys::stat::SFlag;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, PartialOrd, PartialEq)]
/// an object containing information on the object of its path
pub struct Entry {
    /// the path of some file/directory/symlink
    path: PathBuf,
    /// the metadata associated with the file/directory/symlink or None if DNE
    metadata: Option<Stat>,
    /// check of whether the path leads to a symlink (or None if DNE)
    is_link: Option<bool>,
    /// check of whether the path leads to a directory (or None if DNE)
    is_dir: Option<bool>,
}

impl Entry {
    ///
    /// create a new Entry
    ///
    pub fn new(epath: &Path, context: &NetworkContext) -> Self {
        let (metadata, is_link, is_dir) = match context.stat(epath) {
            Ok(stat) => (
                Some(stat),
                Some(stat.mode() & SFlag::S_IFMT.bits() == SFlag::S_IFLNK.bits()),
                Some(stat.mode() & SFlag::S_IFMT.bits() == SFlag::S_IFDIR.bits()),
            ),
            Err(e) => {
                trace!("Error {:?}", e);
                (None, None, None) // note: file DNE
            }
        };
        Entry {
            path: epath.to_path_buf(),
            metadata,
            is_link,
            is_dir,
        }
    }

    /// return the path of the Entry
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// return the metadata associated with the Entry
    pub fn metadata(&self) -> Option<Stat> {
        self.metadata
    }

    /// return the boolean denoting whether the Entry is a symlink
    pub fn is_link(&self) -> Option<bool> {
        self.is_link
    }

    /// return the boolean denoting whether the Entry is a directory
    pub fn is_dir(&self) -> Option<bool> {
        self.is_dir
    }
}
