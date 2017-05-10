[![Version](https://img.shields.io/crates/v/platter-walk.svg)](https://crates.io/crates/platter-walk)

# platter-walk

A recursive directory entry iterator that optimizes traversal based on physical disk layout.
Takes block offsets (via FIEMAP), inode tables and disk cache locality into account.