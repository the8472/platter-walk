//   platter-walk
//   Copyright (C) 2017 The 8472
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
extern crate btrfs;

use btrfs::linux::{get_file_extent_map_for_path};
use std::fs::*;
use std::os::unix::fs::DirEntryExt;
use std::path::PathBuf;
use std::collections::VecDeque;
use std::collections::BTreeMap;
use std::collections::Bound::Included;
use std::error::Error;
use std::io::Write;
use std::path::Path;
use std::os::unix::fs::MetadataExt;

pub struct Entry {
    path: PathBuf,
    ftype: FileType,
    ino: u64
}

impl Entry {
    pub fn new(buf: PathBuf, ft: FileType, ino: u64) -> Entry {
        Entry {
            path: buf,
            ftype: ft,
            ino :ino
        }
    }

    pub fn ino(&self) -> u64 {
        self.ino
    }

    pub fn file_type(&self) -> FileType {
        self.ftype
    }

    pub fn path(&self) -> &Path {
        self.path.as_path()
    }
}

pub struct ToScan {
    phy_sorted : BTreeMap<u64, Entry>,
    phy_sorted_leaves: Vec<(u64, Entry)>,
    unordered : VecDeque<Entry>,
    cursor: u64,
    current_dir: Option<ReadDir>,
    inode_ordered: Vec<Entry>,
    prefilter: Option<Box<Fn(&Path, &FileType) -> bool>>,
    phase: Phase,
    order: Order,
    batch_size: usize
}

#[derive(PartialEq, Copy, Clone)]
pub enum Order {
    /// Return directory entries as they are encountered
    /// Only directories are visited sequentially based on physical layout
    /// This is most useful when file path and type are all the information that's needed
    Dentries,
    /// Return directory entries in batches sorted by inode.
    /// Can be used speed up stat() calls based on the assumption that inode tables are
    /// laid out by ID and thus sequential traversal will be faster.
    Inode,
    /// Return directory entries sorted by physical offset of the file contents
    /// Can be used to get sequential reads over multiple files
    Content
}

#[derive(PartialEq)]
enum Phase {
    DirWalk,
    InodePass,
    ContentPass
}


use Order::*;

impl ToScan {

    pub fn new() -> ToScan {
        ToScan {
            phy_sorted: BTreeMap::new(),
            phy_sorted_leaves: vec![],
            unordered: VecDeque::new(),
            cursor: 0,
            current_dir: None,
            inode_ordered: vec![],
            order: Dentries,
            phase: Phase::DirWalk,
            batch_size: 1024,
            prefilter: None
        }
    }

    pub fn set_order(&mut self, ord: Order) -> &mut Self {
        self.order = ord;
        self
    }

    pub fn set_prefilter(&mut self, filter: Box<Fn(&Path, &FileType) -> bool>) {
        self.prefilter = Some(filter)
    }

    fn is_empty(&self) -> bool {
        self.phy_sorted.is_empty() && self.unordered.is_empty() && self.current_dir.is_none()
    }

    pub fn add_root(&mut self, path : PathBuf) -> std::io::Result<()> {
        let meta = std::fs::metadata(&path)?;
        self.add(Entry{path: path, ino: meta.ino(), ftype: meta.file_type()}, None);
        Ok(())
    }

    fn get_next(&mut self) -> Option<Entry> {
        if !self.unordered.is_empty() {
            return self.unordered.pop_front();
        }

        let next_key = self.phy_sorted.range((Included(&self.cursor), Included(&std::u64::MAX))).next().map(|(k,_)| *k);
        if let Some(k) = next_key {
            self.cursor = k;
            return self.phy_sorted.remove(&k);
        }

        None
    }

    pub fn add(&mut self, to_add : Entry, pos : Option<u64>) {
        match pos {
            Some(idx) => {
                if let Some(old) = self.phy_sorted.insert(idx, to_add) {
                    self.unordered.push_back(old);
                }
            }
            None => {
                self.unordered.push_back(to_add);
            }
        }
    }


}

impl Iterator for ToScan {
    type Item = std::io::Result<Entry>;

    fn next(&mut self) -> Option<std::io::Result<Entry>> {

        while self.phase == Phase::DirWalk && !self.is_empty() {
            if self.current_dir.is_none() {
                let nxt = match self.get_next() {
                    Some(e) => e,
                    None => {
                        self.cursor = 0;
                        continue;
                    }
                };

                match read_dir(nxt.path()) {
                    Ok(dir_iter) => {
                        self.current_dir = Some(dir_iter);
                    },
                    Err(open_err) => return Some(Err(open_err))
                }
            }

            let mut entry = None;

            if let Some(ref mut iter) = self.current_dir {
                entry = iter.next();
            }

            match entry {
                None => {
                    self.current_dir = None;
                    continue;
                }
                Some(Err(e)) => return Some(Err(e)),
                Some(Ok(dent)) => {
                    let meta = match dent.file_type() {
                        Ok(ft) => ft,
                        Err(e) => return Some(Err(e))
                    };

                    // TODO: Better phase-switching?
                    // move to inode pass? won't start the next dir before this one is done anyway
                    if meta.is_dir() {
                        let to_add = Entry::new(dent.path(), meta, dent.ino());
                        //print!{"{} {} ", entry.to_string_lossy(), meta.st_ino()};
                        match get_file_extent_map_for_path(to_add.path()) {
                            Ok(ref extents) if !extents.is_empty() => {
                                self.add(to_add, Some(extents[0].physical));
                            },
                            _ => {
                                // TODO: fall back to inode-order? depth-first?
                                // skip adding non-directories in content order?
                                //self.add(entry, Some(de.ino()))
                                self.add(to_add , None);

                            }
                        }


                    }

                    if let Some(ref filter) = self.prefilter {
                        if !filter(&dent.path(), &meta) {
                            continue;
                        }
                    }

                    match self.order {
                        Order::Dentries => {
                            return Some(Ok(Entry::new(dent.path(), meta, dent.ino())))
                        }
                        Order::Inode | Order::Content => {
                            self.inode_ordered.push(Entry::new(dent.path(), meta, dent.ino()));
                        }
                    }
                }
            }

            if self.inode_ordered.len() >= self.batch_size {
                assert!(self.order != Dentries);
                self.phase = Phase::InodePass;
                // reverse sort so we can pop
                self.inode_ordered.sort_by_key(|dent| std::u64::MAX - dent.ino());
            }
        }


        if self.phase == Phase::InodePass || (self.is_empty() && self.inode_ordered.len() > 0)  {
            assert!(self.inode_ordered.len() > 0);

            match self.order {
                Order::Inode => {
                    let dent = self.inode_ordered.pop().unwrap();
                    if self.inode_ordered.len() == 0 {
                        self.phase = Phase::DirWalk;
                    }
                    return Some(Ok(dent))
                },
                Order::Content => {
                    for e in self.inode_ordered.drain(0..) {
                        let offset = match get_file_extent_map_for_path(e.path()) {
                            Ok(ref extents) if !extents.is_empty() => extents[0].physical,
                            _ => 0
                        };
                        self.phy_sorted_leaves.push((offset, e));
                    }
                    self.phy_sorted_leaves.sort_by_key(|pair| pair.0);
                    self.phase = Phase::ContentPass;
                    assert!(self.phy_sorted_leaves.len() > 0);
                },
                _ => {panic!("illegal state")}
            }

        }

        if self.phase == Phase::ContentPass || (self.is_empty() && self.phy_sorted_leaves.len() > 0) {
            assert!(self.phy_sorted_leaves.len() > 0);
            let dent = self.phy_sorted_leaves.pop().unwrap().1;
            if self.phy_sorted_leaves.len() == 0 {
                self.phase = Phase::DirWalk;
            }
            return Some(Ok(dent))
        }






        None
    }

}

