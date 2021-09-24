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
//! It may raise a [`std::io::Error`] or panic if this assumption is not valid.
//!
//! In the case that your program may not have permission to write to a directory or file
//! in the cache, be sure to check the permissions before modifying any directory or file.
//! The background cleanup thread will panic if it attempts an impermissible write operation.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::TryFutureExt;
use futures::stream::{FuturesUnordered, StreamExt};

mod dir;
mod file;

pub use dir::{DirEntry, DirLock, DirReadGuard, DirWriteGuard};
pub use file::{FileLoad, FileLock, FileReadGuard, FileWriteGuard};

type LFU = freqache::LFUCache<PathBuf>;

struct Inner<FE> {
    files: HashMap<PathBuf, FileLock<FE>>,
    size: usize,
}

impl<FE> Inner<FE> {
    fn new() -> Self {
        Self {
            size: 0,
            files: HashMap::new(),
        }
    }
}

pub struct Cache<FE> {
    capacity: usize,
    lfu: LFU,
    inner: Mutex<Inner<FE>>,
}

impl<FE> Cache<FE> {
    fn insert(&self, path: PathBuf, file: FileLock<FE>, file_size: usize) {
        let mut state = self.inner.lock().expect("file cache state");

        self.lfu.insert(path.clone());

        if state.files.insert(path.clone(), file).is_none() {
            state.size += file_size;
        }
    }

    fn resize(&self, old_size: usize, new_size: usize) {
        let mut state = self.inner.lock().expect("file cache state");

        if new_size > old_size {
            state.size += new_size - old_size;
        } else if new_size < old_size {
            state.size -= old_size - new_size;
        }
    }
}

impl<FE: FileLoad + Send + Sync + 'static> Cache<FE> {
    /// Initialize the cache.
    ///
    /// `cleanup_interval` specifies how often cache cleanup should run in the background.
    ///
    /// This function should only be called once.
    pub fn new(capacity: usize, cleanup_interval: Duration) -> Arc<Self> {
        let cache = Arc::new(Self {
            lfu: LFU::new(),
            inner: Mutex::new(Inner::new()),
            capacity,
        });

        spawn_cleanup_thread(cache.clone(), cleanup_interval);

        cache
    }

    /// Load a filesystem directory into the cache.
    ///
    /// After loading, all interactions with files under this directory should go through
    /// a [`DirLock`] or [`FileLock`].
    pub async fn load(self: Arc<Self>, path: PathBuf) -> Result<DirLock<FE>, io::Error> {
        {
            let lock = self.inner.lock().expect("file cache state");
            for file_path in lock.files.keys() {
                if file_path.starts_with(&path) {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        format!(
                            "called Cache::load on a directory that's already loaded: {:?}",
                            path
                        ),
                    ));
                }
            }
        }

        DirLock::load(self, path).await
    }
}

fn spawn_cleanup_thread<FE: FileLoad + Send + Sync + 'static>(
    cache: Arc<Cache<FE>>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    let mut interval = tokio::time::interval(interval);

    tokio::spawn(async move {
        loop {
            loop {
                if cache.inner.lock().expect("file cache state").size > cache.capacity {
                    break;
                } else {
                    interval.tick().await;
                }
            }

            let mut evictions = {
                let state = cache.inner.lock().expect("file cache state");
                let mut over = state.size as i64 - cache.capacity as i64;
                let evictions = FuturesUnordered::new();

                let mut lfu = cache.lfu.iter();
                while let Some(path) = lfu.next() {
                    if over <= 0 {
                        break;
                    }

                    if let Some(file) = state.files.get(&path).cloned() {
                        if let Some(size) = file.size() {
                            if let Some(eviction) = file.evict() {
                                evictions.push(eviction.map_ok(|()| path));
                                over -= size as i64;
                            }
                        }
                    }
                }

                evictions
            };

            while let Some(result) = evictions.next().await {
                match result {
                    Ok(_path) => {}
                    Err(cause) => panic!("failed to evict file from cache: {}", cause),
                }
            }

            interval.tick().await;
        }
    })
}
