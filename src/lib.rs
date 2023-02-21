//! `freqfs` is an in-memory cache layer over [`tokio::fs`] with least-frequently-used eviction.
//!
//! `freqfs` automatically caches the most frequently-used files and backs up the others to disk.
//! This allows the developer to create and update large collections of data purely in-memory
//! without explicitly sync'ing to disk, while still retaining the flexibility to run on a host
//! with extremely limited memory. This is especially useful for web serving, database,
//! and data science applications.
//!
//! See the [examples](https://github.com/haydnv/freqfs/tree/main/examples) directory for
//! detailed usage examples.
//!
//! This crate assumes that file paths are valid Unicode and may panic if it encounters a file path
//! which is not valid Unicode.
//!
//! It also assumes that all file I/O under the cache root directory (the one whose path is passed
//! to [`load`]) is routed through the cache (not e.g. via [`tokio::fs`] or [`std::fs`] elsewhere).
//! It may raise an [`std::io::Error`] or panic if this assumption is not valid.
//!
//! In the case that your program may not have permission to write to a filesystem entry,
//! be sure to check the permissions before modifying it.
//! The background cleanup thread will panic if it attempts an impermissible write operation.

mod cache;
mod dir;
mod file;

/// The result of a filesystem operation using [`freqfs`]
pub type Result<T> = std::result::Result<T, std::io::Error>;

pub use cache::Cache;
pub use dir::{
    Dir, DirEntry, DirLock, DirReadGuard, DirReadGuardOwned, DirWriteGuard, DirWriteGuardOwned,
};
pub use file::{
    FileLoad, FileLock, FileReadGuard, FileReadGuardOwned, FileWriteGuard, FileWriteGuardOwned,
};
