extern crate libnfs;
extern crate nix;

use libnfs::*;
use std::collections::LinkedList;
use std::path::Path;

fn buildpath(
    entrypath: &str,
    entrytype: EntryType,
    stack: &mut LinkedList<String>,
    parent: &String,
) {
    let p = entrypath;
    let fullpath = format!("{}/{}", parent, p);
    match entrytype {
        EntryType::Directory => {
            if !p.eq(".") && !p.eq("..") {
                stack.push_front(fullpath);
            }
        }
        _ => println!("{:?}", fullpath),
    }
}

fn entry_to_string(entry: DirEntry, stack: &mut LinkedList<String>, parent: &String) {
    let f = entry;
    match f.path.to_str() {
        Some(entrypath) => buildpath(entrypath, f.d_type, stack, parent),
        None => error!("Error, non-unicode character in file path"),
    }
}

fn dir_loop(dir: NfsDirectory, stack: &mut LinkedList<String>, parent: &String) {
    for file in dir {
        match file {
            Ok(entry) => entry_to_string(entry, stack, parent),
            Err(e) => error!("Error! {:?}", e),
        };
    }
}

fn create_nfs(uid: i32, gid: i32, level: i32, ip: &str, root: &str) -> std::io::Result<Nfs> {
    let nfs = Nfs::new()?;
    nfs.set_uid(uid)?;
    nfs.set_gid(gid)?;
    nfs.set_debug(level)?;
    nfs.mount(ip, root)?;
    Ok(nfs)
}

fn list_files(nfs: &mut Nfs) -> std::io::Result<()>
{
    let mut stack: LinkedList<String> = LinkedList::new();
    let dir = nfs.opendir(&Path::new("/"))?;
    dir_loop(dir, &mut stack, &"".to_string());
    while !stack.is_empty() {
        let p = stack.pop_front().unwrap();
        println!("{:?}", p);
        let dir = nfs.opendir(Path::new(&p))?;
        dir_loop(dir, &mut stack, &p);
    }
    Ok(())
}