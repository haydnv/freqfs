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
//! In the case that your program may not have permission to write to a filesystem entry,
//! be sure to check the permissions before modifying it.
//! The background cleanup thread will panic if it attempts an impermissible write operation.

use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use futures::future::Future;
use futures::stream::{FuturesUnordered, StreamExt};

mod dir;
mod file;

pub use dir::{Dir, DirEntry, DirLock, DirReadGuard, DirWriteGuard};
pub use file::{FileLoad, FileLock, FileReadGuard, FileWriteGuard};

const MAX_FILE_HANDLES: usize = 512;

type LFU<FE> = freqache::LFUCache<PathBuf, FileLock<FE>>;

struct State<FE> {
    files: LFU<FE>,
    size: usize,
}

impl<FE> State<FE> {
    fn new() -> Self {
        Self {
            size: 0,
            files: LFU::new(),
        }
    }
}

/// An in-memory cache layer over [`tokio::fs`] with least-frequently-used (LFU) eviction.
pub struct Cache<FE> {
    capacity: usize,
    max_file_handles: usize,
    state: Mutex<State<FE>>,
}

impl<FE> Cache<FE> {
    fn bump(&self, path: &PathBuf, file_size: usize, newly_loaded: bool) -> bool {
        let mut state = self.lock();

        if newly_loaded {
            state.size += file_size;
        }

        state.files.bump(path)
    }

    fn insert(&self, path: PathBuf, file: FileLock<FE>, file_size: usize) {
        let mut state = self.lock();
        state.files.insert(path, file);
        state.size += file_size;
    }

    fn lock(&self) -> MutexGuard<State<FE>> {
        self.state.lock().expect("file cache state")
    }

    fn remove(&self, path: &PathBuf, size: usize) {
        let mut state = self.lock();
        if state.files.remove(path).is_some() {
            state.size -= size;
        }
    }

    fn resize(&self, old_size: usize, new_size: usize) {
        let mut state = self.lock();
        state.size += new_size;
        state.size -= old_size;
    }
}

impl<FE: FileLoad + Send + Sync + 'static> Cache<FE> {
    /// Initialize the cache.
    ///
    /// `cleanup_interval` specifies how often cache cleanup should run in the background.
    /// `max_file_handles` specifies how many files are allowed to be evicted at once.
    /// If not specified, `max_file_handles` will default to 512.
    ///
    /// This function should only be called once.
    ///
    /// Panics: if `max_file_handles` is `Some(0)`
    pub fn new(
        capacity: usize,
        cleanup_interval: Duration,
        max_file_handles: Option<usize>,
    ) -> Arc<Self> {
        let max_file_handles = max_file_handles.unwrap_or(MAX_FILE_HANDLES);

        let cache = Arc::new(Self {
            capacity,
            max_file_handles,
            state: Mutex::new(State::new()),
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
            let state = self.lock();
            for (file_path, _) in state.files.iter() {
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

    fn gc(&self) -> FuturesUnordered<impl Future<Output = Result<(), io::Error>>> {
        let evictions = FuturesUnordered::new();
        let state = self.lock();
        if state.size < self.capacity {
            return evictions;
        }

        let mut over = state.size as i64 - self.capacity as i64;

        for (_path, file) in state.files.iter() {
            if let Some((size, eviction)) = file.clone().evict() {
                over -= size as i64;
                evictions.push(eviction);
            }

            if over <= 0 || evictions.len() >= self.max_file_handles {
                break;
            }
        }

        evictions
    }
}

fn spawn_cleanup_thread<FE: FileLoad + Send + Sync + 'static>(
    cache: Arc<Cache<FE>>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let mut evictions = cache.gc();

            if evictions.is_empty() {
                tokio::time::sleep(interval).await
            } else {
                while let Some(result) = evictions.next().await {
                    match result {
                        Ok(()) => {}
                        Err(cause) => panic!("failed to evict file from cache: {}", cause),
                    }
                }
            }
        }
    })
}
