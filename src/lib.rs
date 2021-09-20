use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

mod dir;
mod file;

pub use dir::{DirEntry, DirLock, DirReadGuard, DirWriteGuard};
pub use file::{FileEntry, FileLoad, FileLock, FileReadGuard, FileWriteGuard};

type LFU = freqache::LFUCache<PathBuf>;

struct Evict;

struct Cache {
    lfu: LFU,
    size: Mutex<usize>,
    capacity: usize,
    tx: mpsc::UnboundedSender<Evict>,
}

impl Cache {
    fn insert(&self, path: PathBuf, file_size: usize) {
        let mut size = self.size.lock().expect("file cache size");

        if !self.lfu.insert(path) {
            *size += file_size;
        }

        if &*size > &self.capacity {
            if let Err(cause) = self.tx.send(Evict) {
                panic!("filesystem cache cleanup thread is dead: {}", cause);
            }
        }
    }
}

impl Cache {
    fn new(capacity: usize, tx: mpsc::UnboundedSender<Evict>) -> Self {
        let size = Mutex::new(0);
        let lfu = LFU::new();

        Self {
            lfu,
            size,
            capacity,
            tx,
        }
    }
}

pub async fn load<FE>(root: PathBuf, cache_size: usize) -> Result<DirLock<FE>, io::Error> {
    let (tx, rx) = mpsc::unbounded_channel();
    let cache = Arc::new(Cache::new(cache_size, tx));
    spawn_cleanup_thread(cache.clone(), rx);
    DirLock::load(cache, root).await
}

fn spawn_cleanup_thread(_cache: Arc<Cache>, mut rx: mpsc::UnboundedReceiver<Evict>) {
    tokio::spawn(async move {
        while rx.recv().await.is_some() {
            unimplemented!()
        }
    });
}
