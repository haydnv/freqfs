use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::stream::{FuturesUnordered, StreamExt};

mod dir;
mod file;

pub use dir::{DirEntry, DirLock, DirReadGuard, DirWriteGuard};
pub use file::{FileEntry, FileLoad, FileLock, FileReadGuard, FileWriteGuard};

type LFU = freqache::LFUCache<PathBuf>;

struct Inner<FE> {
    files: HashMap<PathBuf, FileLock<FE>>,
    size: usize,
}

struct Cache<FE> {
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

    fn remove(&self, path: &PathBuf, entry_size: usize) {
        let mut state = self.inner.lock().expect("file cache state");

        if state.files.remove(path).is_some() {
            self.lfu.remove(path);
            state.size -= entry_size;
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

impl<FE> Cache<FE> {
    fn new(capacity: usize) -> Self {
        let inner = Mutex::new(Inner {
            size: 0,
            files: HashMap::new(),
        });

        Self {
            lfu: LFU::new(),
            inner,
            capacity,
        }
    }
}

pub async fn load<FE: FileLoad + Send + Sync + 'static>(
    root: PathBuf,
    cache_size: usize,
    cleanup_interval: Duration,
) -> Result<DirLock<FE>, io::Error> {
    let cache = Arc::new(Cache::new(cache_size));
    spawn_cleanup_thread(cache.clone(), cleanup_interval);
    let dir = DirLock::load(cache, root).await?;
    Ok(dir)
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
                let evictions = FuturesUnordered::new();
                let occupied = cache.inner.lock().expect("file cache state").size;
                let mut over = occupied as i64 - cache.capacity as i64;

                let mut lfu = cache.lfu.iter();
                while let Some(path) = lfu.next() {
                    if over <= 0 {
                        break;
                    }

                    let state = cache.inner.lock().expect("file cache state");
                    if let Some(file) = state.files.get(&path).cloned() {
                        if let Some(size) = file.size() {
                            if let Some(eviction) = file.evict() {
                                evictions.push(eviction);
                                over -= size as i64;
                            }
                        }
                    } else {
                        // since we're not holding the lock on `state`,
                        // the file may already have been removed
                        // between calling lfu.iter() and now
                    }
                }

                evictions
            };

            while let Some(result) = evictions.next().await {
                match result {
                    Ok(()) => {}
                    Err(cause) => panic!("failed to evict file from cache: {}", cause),
                }
            }

            interval.tick().await;
        }
    })
}
