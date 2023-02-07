use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use futures::future::Future;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use super::dir::DirLock;
use super::file::{FileLoad, FileLock};

const MAX_FILE_HANDLES: usize = 512;

type LFU<FE> = ds_ext::LinkedHashMap<PathBuf, FileLock<FE>>;

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

#[derive(Debug)]
struct Evict;

/// An in-memory cache layer over [`tokio::fs`] with least-frequently-used (LFU) eviction.
pub struct Cache<FE> {
    capacity: usize,
    max_file_handles: usize,
    state: Mutex<State<FE>>,
    tx: UnboundedSender<Evict>,
}

impl<FE> Cache<FE> {
    #[inline]
    fn check(&self, state: MutexGuard<State<FE>>) {
        if state.size > self.capacity {
            self.tx.send(Evict).expect("cache cleanup thread");
        }
    }

    #[inline]
    fn lock(&self) -> MutexGuard<State<FE>> {
        self.state.lock().expect("file cache state")
    }

    pub(crate) fn bump(&self, path: &PathBuf, file_size: usize, newly_loaded: bool) -> bool {
        let mut state = self.lock();

        if newly_loaded {
            state.size += file_size;
        }

        let exists = state.files.bump(path);
        self.check(state);
        exists
    }

    pub(crate) fn insert(&self, path: PathBuf, file: FileLock<FE>, file_size: usize) {
        let mut state = self.lock();
        state.files.insert(path, file);
        state.size += file_size;

        self.check(state)
    }

    pub(crate) fn remove(&self, path: &PathBuf, size: usize) {
        let mut state = self.lock();

        if state.files.remove(path).is_some() {
            state.size -= size;
        }

        self.check(state)
    }

    pub(crate) fn resize(&self, old_size: usize, new_size: usize) {
        let mut state = self.lock();
        state.size += new_size;
        state.size -= old_size;

        self.check(state)
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
    pub fn new(capacity: usize, max_file_handles: Option<usize>) -> Arc<Self> {
        let max_file_handles = max_file_handles.unwrap_or(MAX_FILE_HANDLES);
        let (tx, rx) = mpsc::unbounded_channel();

        let cache = Arc::new(Self {
            capacity,
            max_file_handles,
            state: Mutex::new(State::new()),
            tx,
        });

        spawn_cleanup_thread(cache.clone(), rx);

        cache
    }

    /// Load a filesystem directory into the cache.
    ///
    /// After loading, all interactions with files under this directory should go through
    /// a [`DirLock`] or [`FileLock`].
    pub fn load(self: Arc<Self>, path: PathBuf) -> Result<DirLock<FE>, io::Error> {
        {
            let state = self.lock();
            for (file_path, _) in state.files.iter() {
                if file_path.starts_with(&path) || path.starts_with(file_path) {
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

        DirLock::load(self, path)
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

fn spawn_cleanup_thread<FE: FileLoad>(
    cache: Arc<Cache<FE>>,
    mut rx: UnboundedReceiver<Evict>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(Evict) = rx.recv().await {
            let mut evictions = cache.gc();

            while let Some(result) = evictions.next().await {
                match result {
                    Ok(()) => {}
                    Err(cause) => panic!("failed to evict file from cache: {}", cause),
                }
            }
        }
    })
}
