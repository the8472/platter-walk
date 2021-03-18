//   platter-walk
//   Copyright (C) 2017 The 8472
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
extern crate btrfs2 as btrfs;
extern crate mnt;
extern crate libc;

use btrfs::linux::{get_file_extent_map_for_path, FileExtent};
use std::fs::*;
use std::os::unix::fs::DirEntryExt;
use std::path::PathBuf;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ops::Bound::{Included, Excluded};
use std::path::Path;
use std::os::unix::fs::MetadataExt;
use std::os::unix::io::AsRawFd;

pub struct Entry {
    path: PathBuf,
    ftype: FileType,
    ino: u64,
    extents: Vec<FileExtent>,
}

impl Entry {
    pub fn new(buf: PathBuf, ft: FileType, ino: u64, extents: Vec<FileExtent>) -> Entry {
        Entry {
            path: buf,
            ftype: ft,
            ino,
            extents
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

    fn extent_sum(&self) -> u64 {
        self.extents.iter().map(|e| e.length).sum()
    }
}

impl PartialEq for Entry {
    fn eq(&self, other: &Entry) -> bool {
        self.path.eq(&other.path)
    }
}

impl PartialEq<Path> for Entry {
    fn eq(&self, p: &Path) -> bool {
        self.path.eq(p)
    }
}

pub struct ToScan {
    phy_sorted : BTreeMap<u64, Entry>,
    phy_sorted_leaves: Vec<(u64, Entry)>,
    unordered : VecDeque<Entry>,
    cursor: u64,
    current_dir: Option<ReadDir>,
    inode_ordered: Vec<Entry>,
    prefilter: Option<Box<dyn Fn(&Path, &FileType) -> bool>>,
    phase: Phase,
    order: Order,
    batch_size: usize,
    prefetched: HashMap<PathBuf, u64>,
    mountpoints: Vec<mnt::MountEntry>,
    prefetch_cap: usize
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
            prefilter: None,
            prefetched: Default::default(),
            mountpoints: vec![],
            prefetch_cap: 0
        }
    }

    pub fn set_order(&mut self, ord: Order) -> &mut Self {
        self.order = ord;
        self
    }

    pub fn prefetch_dirs(&mut self, val: bool) {
        if !val {
            self.mountpoints = vec![];
            return;
        }

        self.mountpoints = match mnt::MountIter::new_from_proc() {
            Ok(m) => m,
            Err(_) => {
                self.mountpoints = vec![];
                return
            }
        }.filter_map(|e| e.ok()).collect();
    }

    pub fn set_prefilter(&mut self, filter: Box<dyn Fn(&Path, &FileType) -> bool>) {
        self.prefilter = Some(filter)
    }

    pub fn set_batchsize(&mut self, batch: usize) {
        self.batch_size = batch;
    }

    fn is_empty(&self) -> bool {
        self.phy_sorted.is_empty() && self.unordered.is_empty() && self.current_dir.is_none()
    }

    pub fn add_root(&mut self, path : PathBuf) -> std::io::Result<()> {
        let meta = std::fs::metadata(&path)?;
        self.add(Entry{path, ino: meta.ino(), ftype: meta.file_type(), extents: vec![]}, None);
        Ok(())
    }

    fn get_next(&mut self) -> Option<Entry> {
        self.prefetch();

        if !self.unordered.is_empty() {
            let res = self.unordered.pop_front();
            self.remove_prefetch(&res);
            return res;
        }

        let next_key = self.phy_sorted.range((Included(&self.cursor), Included(&u64::MAX))).next().map(|(k,_)| *k);
        if let Some(k) = next_key {
            self.cursor = k;
            let res = self.phy_sorted.remove(&k);
            self.remove_prefetch(&res);
            return res;
        }

        None
    }

    fn remove_prefetch(&mut self, e : &Option<Entry>) {
        if let Some(ref e) = *e {
            if self.prefetched.remove(e.path()).is_some() {
                self.prefetch_cap = std::cmp::min(2048,self.prefetch_cap * 2 + 1);
            } else {
                self.prefetch_cap = 2;
                self.prefetched.clear();
            }

        }
    }

    fn prefetch(&mut self) {
        if self.mountpoints.is_empty() {
            return;
        }

        const LIMIT : u64 = 8*1024*1024;

        let consumed = self.prefetched.iter().map(|ref tuple| tuple.1).sum::<u64>();
        let mut remaining = LIMIT.saturating_sub(consumed);
        let prev_fetched = self.prefetched.len();

        // hysteresis
        if remaining < LIMIT/2 {
            return;
        }

        let unordered_iter = self.unordered.iter();
        let ordered_iter_front = self.phy_sorted.range((Included(&self.cursor), Included(&u64::MAX))).map(|(_,v)| v);
        let ordered_iter_tail = self.phy_sorted.range((Included(&0), Excluded(&self.cursor))).map(|(_,v)| v);

        let mut prune = vec![];

        {
            let mut device_groups = HashMap::new();

            for e in unordered_iter.chain(ordered_iter_front).chain(ordered_iter_tail) {
                if remaining == 0 {
                    break;
                }

                if self.prefetched.len() > self.prefetch_cap + 1 {
                    break;
                }

                if self.prefetched.contains_key(e.path()) {
                    continue;
                }

                let size = e.extent_sum();
                remaining = remaining.saturating_sub(size);
                self.prefetched.insert(e.path().to_owned(), size);

                let mount = self.mountpoints.iter().rev().find(|mnt| e.path().starts_with(&mnt.file));

                // TODO: only try to open devices once
                match mount {
                    Some(&mnt::MountEntry {ref spec, ref vfstype, ..})
                    if vfstype == "ext4" || vfstype == "ext3"
                    => {
                        let mount_slot = device_groups.entry(spec).or_insert(vec![]);
                        mount_slot.extend(&e.extents);
                    }
                    _ => {}
                }
            }

            for (p, extents) in device_groups {
                let mut ordered_extents = extents.to_vec();
                ordered_extents.sort_by_key(|e| e.physical);

                if let Ok(f) = File::open(p) {

                    let mut i = 0;

                    while i < ordered_extents.len() {
                        let ext1 = ordered_extents[i];
                        let offset = ext1.physical;
                        let mut end = offset + ext1.length;

                        for j in i+1..ordered_extents.len() {
                            let ref ext2 = ordered_extents[j];
                            if ext2.physical > end {
                                break;
                            }

                            i = j;

                            end = ext2.physical+ext2.length;
                        }

                        i+=1;

                        unsafe {
                            libc::posix_fadvise(f.as_raw_fd(), offset as i64, (end - offset) as i64, libc::POSIX_FADV_WILLNEED);
                        }
                    }
                } else {
                    prune.push(p.to_owned());
                }
            }

        }

        //println!("bytes: {} -> {}, f: {}->{}, sc: {}", LIMIT-consumed, remaining, prev_fetched ,self.prefetched.len(), self.prefetch_cap);

        if !prune.is_empty() {
            self.mountpoints.retain(|e| prune.contains(&e.spec));
        }


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

                        let extents = match get_file_extent_map_for_path(dent.path()) {
                            Ok(extents) => extents,
                            _ => vec![]
                        };

                        let to_add = Entry::new(dent.path(), meta, dent.ino(), extents);

                        if !to_add.extents.is_empty() {
                            let offset = to_add.extents[0].physical;
                            self.add(to_add, Some(offset));
                        } else {
                            // TODO: fall back to inode-order? depth-first?
                            // skip adding non-directories in content order?
                            self.add(to_add, None);
                        }


                    }

                    if let Some(ref filter) = self.prefilter {
                        if !filter(&dent.path(), &meta) {
                            continue;
                        }
                    }

                    match self.order {
                        Order::Dentries => {
                            return Some(Ok(Entry::new(dent.path(), meta, dent.ino(), vec![])))
                        }
                        Order::Inode | Order::Content => {
                            self.inode_ordered.push(Entry::new(dent.path(), meta, dent.ino(), vec![]));
                        }
                    }
                }
            }

            if self.inode_ordered.len() >= self.batch_size {
                assert!(self.order != Dentries);
                self.phase = Phase::InodePass;
                // reverse sort so we can pop
                self.inode_ordered.sort_by_key(|dent| u64::MAX - dent.ino());
            }
        }


        if self.phase == Phase::InodePass || (self.is_empty() && !self.inode_ordered.is_empty())  {
            assert!(!self.inode_ordered.is_empty());

            match self.order {
                Order::Inode => {
                    let dent = self.inode_ordered.pop().unwrap();
                    if self.inode_ordered.is_empty() {
                        self.phase = Phase::DirWalk;
                    }
                    return Some(Ok(dent))
                },
                Order::Content => {
                    for e in self.inode_ordered.drain(0..).rev() {
                        let offset = match get_file_extent_map_for_path(e.path()) {
                            Ok(ref extents) if !extents.is_empty() => extents[0].physical,
                            _ => 0
                        };
                        self.phy_sorted_leaves.push((offset, e));
                    }
                    self.phy_sorted_leaves.sort_by_key(|pair| pair.0);
                    self.phase = Phase::ContentPass;
                    assert!(!self.phy_sorted_leaves.is_empty());
                },
                _ => {panic!("illegal state")}
            }

        }

        if self.phase == Phase::ContentPass || (self.is_empty() && !self.phy_sorted_leaves.is_empty()) {
            assert!(!self.phy_sorted_leaves.is_empty());
            let dent = self.phy_sorted_leaves.pop().unwrap().1;
            if self.phy_sorted_leaves.is_empty() {
                self.phase = Phase::DirWalk;
            }
            return Some(Ok(dent))
        }

        None
    }

}

