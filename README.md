[![Version](https://img.shields.io/crates/v/platter-walk.svg)](https://crates.io/crates/platter-walk)

# platter-walk

A recursive directory entry iterator that optimizes traversal based on physical disk layout.
Takes block offsets (via FIEMAP[[1]](https://github.com/torvalds/linux/commit/abc8746eb91fb01e8d411896f80f7687c0d8372e)), inode tables and disk cache locality into account.

For users (root) who have read access to the underlying block device it also performs readaheads on the directory indicies
spanning several directories. This is somewhat of a hack since `readahead()` and `posix_fadvise()` do not work on directories directly
since they use a separate cache.[[2]](https://www.spinics.net/lists/linux-fsdevel/msg30843.html)[[3]](https://www.spinics.net/lists/linux-fsdevel/msg31321.html)

The largest benefits can be realized on HDDs with ext4 filesystems.

Traversal can be optimized for

* simple directory entry listing (name and `d_type` only)
* detailed entry listing (`stat`)
* reading file contents. Entry batches are sorted by physical offset.

See [ffcnt](https://github.com/the8472/ffcnt#unscientific-benchmark) and [fastar](https://github.com/the8472/fastar#benchmarks) for benchmarks.

