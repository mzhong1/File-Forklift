use crate::filesystem::*;
use log::*;
use nix::sys::stat::SFlag;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, PartialOrd, PartialEq)]
pub struct Entry {
    path: PathBuf,
    metadata: Option<Stat>,
    is_link: Option<bool>,
    is_dir: Option<bool>,
}

impl Entry {
    pub fn new(epath: &Path, context: &NetworkContext) -> Self {
        let (metadata, is_link, is_dir) = match context.stat(epath) {
            Ok(stat) => (
                Some(stat),
                Some(stat.mode() & SFlag::S_IFMT.bits() == SFlag::S_IFLNK.bits()),
                Some(stat.mode() & SFlag::S_IFMT.bits() == SFlag::S_IFDIR.bits()),
            ),
            Err(e) => {
                debug!("Error {:?}", e);
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

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn metadata(&self) -> Option<Stat> {
        self.metadata
    }

    pub fn is_link(&self) -> Option<bool> {
        self.is_link
    }

    pub fn is_dir(&self) -> Option<bool> {
        self.is_dir
    }
}
