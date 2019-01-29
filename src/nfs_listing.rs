use crossbeam;
use libnfs;

use rayon;


use self::libnfs::*;
use self::rayon::ThreadPoolBuilder;
use crossbeam::channel;
use std::collections::LinkedList;
use std::io::Result;
use std::path::{Path, PathBuf};
use std::thread;

fn buildpath(entrypath: &str, entrytype: EntryType, stack: &mut LinkedList<String>, parent: &str) {
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

fn entry_to_string(entry: DirEntry, stack: &mut LinkedList<String>, parent: &str) {
    let f = entry;
    match f.path.to_str() {
        Some(entrypath) => buildpath(entrypath, f.d_type, stack, parent),
        None => error!("Error, non-unicode character in file path"),
    }
}

fn dir_loop(dir: NfsDirectory, stack: &mut LinkedList<String>, parent: &str) {
    for file in dir {
        match file {
            Ok(entry) => entry_to_string(entry, stack, parent),
            Err(e) => error!("Error! {:?}", e),
        };
    }
}

fn create_nfs(uid: i32, gid: i32, level: i32, ip: &str, root: &str) -> Result<Nfs> {
    let nfs = Nfs::new()?;
    nfs.set_uid(uid)?;
    nfs.set_gid(gid)?;
    nfs.set_debug(level)?;
    nfs.mount(ip, root)?;
    Ok(nfs)
}

fn list_files(nfs: &mut Nfs) -> Result<()> {
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

fn thread_buildpath(entrypath: &str, entrytype: EntryType, parent: &str) -> String {
    let p = entrypath;
    let fullpath = format!("{}/{}", parent, p);
    match entrytype {
        EntryType::Directory => {
            if !p.eq(".") && !p.eq("..") {
                fullpath
            } else {
                "".to_string()
            }
        }
        _ => {
            println!("{:?}", fullpath);
            "".to_string()
        }
    }
}

fn thread_entry_to_string(entry: DirEntry, parent: &str) -> String {
    let f = entry;
    match f.path.to_str() {
        Some(entrypath) => thread_buildpath(entrypath, f.d_type, parent),
        None => {
            error!("Error, non-unicode character in file path");
            "".to_string()
        }
    }
}
fn thread_dir_loop(dir: NfsDirectory, parent: &str) -> Vec<String> {
    let mut nodes: Vec<String> = Vec::new();
    for file in dir {
        match file {
            Ok(entry) => {
                let ret = thread_entry_to_string(entry, parent);
                if !ret.eq("") {
                    nodes.push(ret)
                }
            }
            Err(e) => error!("Error! {:?}", e),
        };
    }
    nodes
}

fn is_empty(d: &NfsDirectory) -> bool {
    let this = Path::new(".");
    let parent = Path::new("..");
    let mut ret_val = true;
    for dir_entry in d.clone() {
        let dir_entry = dir_entry.unwrap();
        if dir_entry.path == this || dir_entry.path == parent {
            continue;
        }
        match dir_entry.d_type {
            // If there's anything in here besides . or .. then return false
            _ => {
                trace!("{:?} is not empty", dir_entry);
                ret_val = false;
            }
        }
    }

    ret_val
}

fn thread_exp2(nodes: Vec<String>) {
    rayon::scope(|spawner| {
        for file in nodes {
            println!("{:?}", file);
            let vec = thread_dir_loop(
                create_nfs(1001, 1001, 9, "192.168.122.89", "/squish")
                    .unwrap()
                    .opendir(Path::new(&file))
                    .unwrap(),
                &file,
            );
            spawner.spawn(|_| {
                println!("Current thread id {:?}", thread::current().id());
                thread_exp2(vec);
            });
        }
    });
}

fn t_traversal(uid: i32, gid: i32, level: i32, ip: &str, root: &str, path: &str) -> Result<()> {
    rayon::scope(|spawner| {
        let mut nfs = create_nfs(uid, gid, level, ip, root).unwrap();
        let dir = nfs.opendir(&Path::new(&path)).unwrap();
        for file in dir {
            match file {
                Ok(entry) => {
                    let f = entry;
                    match f.path.to_str() {
                        Some(entrypath) => {
                            let p = entrypath;
                            let fullpath = {
                                if path.eq("/") {
                                    format!("/{}", p)
                                } else {
                                    format!("{}/{}", path, p)
                                }
                            };
                            let entrytype = f.d_type;
                            match entrytype {
                                EntryType::Directory => {
                                    if !p.eq(".") && !p.eq("..") {
                                        spawner.spawn(|_| {
                                            let fullpath = fullpath;
                                            t_traversal(uid, gid, level, ip, root, &fullpath)
                                                .unwrap();
                                        });
                                    }
                                }
                                _ => println!("{:?}", fullpath),
                            }
                        }
                        None => error!("Error, non-unicode character in file path"),
                    }
                }
                Err(e) => error!("Error! {:?}", e),
            };
        }
    });

    Ok(())
}

fn linear_thread_lister(uid: i32, gid: i32, _ip: &str, _root: &str, num_threads: usize) {
    let (tx, rx) = channel::unbounded();
    let _pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .unwrap();

    //1 thread to fill channel
    let handle = thread::spawn(move || {
        let mut done = false;
        let mut stack: Vec<PathBuf> = vec![PathBuf::from("/")];
        let this = Path::new(".");
        let parent = Path::new("..");
        let mut nfs = Nfs::new().unwrap();
        nfs.set_uid(uid).unwrap();
        nfs.set_gid(gid).unwrap();
        nfs.mount("192.168.122.89", "/squish").unwrap();
        while !done {
            match stack.pop() {
                Some(p) => {
                    let dir = nfs.opendir(&p).unwrap();
                    for f in dir {
                        let f = f.expect("File failed");
                        if f.path == this || f.path == parent {
                            println!("Skipping . or ..");
                            continue;
                        }
                        match f.d_type {
                            EntryType::Directory => {
                                println!("dir: {:?}", f.path.display());
                                stack.push(p.join(f.path.clone()));
                                tx.send(Some(p.join(f.path)));
                            }
                            _ => {}
                        }
                    }
                }
                None => {
                    done = true;
                    tx.send(None);
                }
            }
        }
        println!("FISRT WHILE DONE");
    });

    let mut processing_done = false;
    while !processing_done {
        for _ in 0..(num_threads - 1) {
            let rx = rx.clone();
            rayon::scope(|s| {
                s.spawn(|_s| {
                    match rx.recv() {
                        Ok(m) => {
                            let mut nfs = Nfs::new().unwrap();
                            nfs.set_uid(uid).unwrap();
                            nfs.set_gid(gid).unwrap();
                            nfs.mount("192.168.122.89", "/squish").unwrap();
                            match m {
                                Some(p) => {
                                    println!("opening: {}", p.display());
                                    let dir = nfs.opendir(&p).unwrap();
                                    for _f in dir {}
                                }
                                None => {
                                    processing_done = true;
                                }
                            }
                        }
                        Err(_) => processing_done = true,
                    };
                });
            });
        }
    }
    println!("Finished processing!");
    handle.join().unwrap();
}
