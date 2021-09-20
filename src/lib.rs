use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

#[allow(unused)]
mod dir;
#[allow(unused)]
mod file;

pub use dir::{DirEntry, DirLock};
pub use file::{File, FileEntry, FileLock};

type LFU = freqache::LFUCache<PathBuf>;

struct Evict;

#[allow(unused)]
struct Cache {
    lfu: LFU,
    size: Mutex<usize>,
    capacity: usize,
    tx: mpsc::UnboundedSender<Evict>,
}

impl Cache {
    #[allow(unused)]
    fn insert(&self, path: PathBuf, file_size: usize) {
        let mut size = self.size.lock().expect("file cache size");
        *size += file_size;

        self.lfu.insert(path);

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

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;
    use std::path::PathBuf;

    #[allow(unused)]
    enum FileEnum {
        Bin(Vec<u8>),
        Text(String),
    }

    impl FileEntry<Vec<u8>> for FileEnum {
        fn as_file(&self) -> Option<&Vec<u8>> {
            match self {
                Self::Bin(bytes) => Some(bytes),
                _ => None,
            }
        }

        fn as_file_mut(&mut self) -> Option<&mut Vec<u8>> {
            match self {
                Self::Bin(bytes) => Some(bytes),
                _ => None,
            }
        }
    }

    impl FileEntry<String> for FileEnum {
        fn as_file(&self) -> Option<&String> {
            match self {
                Self::Text(text) => Some(text),
                _ => None,
            }
        }

        fn as_file_mut(&mut self) -> Option<&mut String> {
            match self {
                Self::Text(text) => Some(text),
                _ => None,
            }
        }
    }

    #[tokio::test]
    async fn test_load() {
        let mut rng = rand::thread_rng();
        let path = loop {
            let rand: u32 = rng.gen();
            let path = PathBuf::from(format!("/tmp/test_freqfs_{}", rand));
            if !path.exists() {
                tokio::fs::create_dir(&path).await.expect("tmp test dir");
                break path;
            }
        };

        load::<FileEnum>(path.clone(), 1000).await.expect("cache");

        tokio::fs::remove_dir_all(path)
            .await
            .expect("tmp test dir cleanup");
    }
}
