[![Version](https://img.shields.io/crates/v/platter-walk.svg)](https://crates.io/crates/platter-walk)

# platter-walk

A recursive directory entry iterator that optimizes traversal based on physical disk layout.
Takes block offsets (via FIEMAP), inode tables and disk cache locality into account.

The largest benefits can be realized on HDDs with ext4 filesystems.

Traversal can be optimized for

* simple directory entry listing (name and `d_type` only)
* detailed entry listing (`stat`)
* reading file contents. Entry batches are sorted by physical offset.

See [ffcnt](https://github.com/the8472/ffcnt#unscientific-benchmark) and [fastar](https://github.com/the8472/fastar#benchmarks) for benchmarks.

