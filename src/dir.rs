use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::Hash;
use std::io;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use futures::future::{Future, FutureExt};
use futures::stream::{FuturesUnordered, StreamExt, TryStreamExt};
use log::warn;
use tokio::fs;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

use crate::file::FileLock;
use crate::Cache;

/// A directory entry, either a [`FileLock`] or a sub-[`DirLock`].
#[derive(Clone)]
pub enum DirEntry<FE> {
    Dir(DirLock<FE>),
    File(FileLock<FE>),
}

impl<FE> DirEntry<FE> {
    /// return `Some(dir_lock)` if this `DirEntry` is itself a directory.
    pub fn as_dir(&self) -> Option<&DirLock<FE>> {
        match self {
            Self::Dir(dir) => Some(dir),
            _ => None,
        }
    }

    /// return `Some(file_lock)` if this `DirEntry` is itself a file.
    pub fn as_file(&self) -> Option<&FileLock<FE>> {
        match self {
            Self::File(file) => Some(file),
            _ => None,
        }
    }
}

/// A filesystem directory
pub struct Dir<FE> {
    path: PathBuf,
    cache: Arc<Cache<FE>>,
    contents: HashMap<String, DirEntry<FE>>,
    deleted: HashSet<String>,
}

impl<FE> Dir<FE> {
    /// Borrow the [`Path`] of this [`Dir`].
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    /// Return `true` if this [`Dir`] has an entry with the given `name`.
    pub fn contains<N: Borrow<str>>(&self, name: N) -> bool {
        if self.deleted.contains(name.borrow()) {
            false
        } else {
            self.contents.contains_key(name.borrow())
        }
    }

    /// Create and return a new subdirectory of this [`Dir`].
    pub fn create_dir(&mut self, name: String) -> Result<DirLock<FE>, io::Error> {
        if !self.deleted.remove(&name) {
            if self.contents.contains_key(&name) {
                warn!(
                    "attempted to create a directory {} in {:?} that already exists",
                    name, self.path
                );

                return Err(io::Error::new(io::ErrorKind::AlreadyExists, name));
            }
        }

        let mut path = self.path.clone();
        path.push(&name);
        let lock = DirLock::new(self.cache.clone(), path);
        self.contents.insert(name, DirEntry::Dir(lock.clone()));
        Ok(lock)
    }

    /// Create a new file in this [`Dir`] with the given `contents`.
    pub fn create_file<F>(
        &mut self,
        name: String,
        contents: F,
        size: usize,
    ) -> Result<FileLock<FE>, io::Error>
    where
        FE: From<F>,
    {
        if !self.deleted.remove(&name) {
            if self.contents.contains_key(&name) {
                warn!(
                    "attempted to create a file {} in {:?} that already exists",
                    name, self.path
                );
                return Err(io::Error::new(io::ErrorKind::AlreadyExists, name));
            }
        }

        let mut path = self.path.clone();
        path.push(&name);

        let lock = FileLock::new(self.cache.clone(), path.clone(), contents, size);
        self.contents.insert(name, DirEntry::File(lock.clone()));
        self.cache.insert(path, lock.clone(), size);
        Ok(lock)
    }

    /// Return a new subdirectory of this [`Dir`], creating it if it doesn't already exist.
    pub fn get_or_create_dir(&mut self, name: String) -> Result<DirLock<FE>, io::Error> {
        // if the requested dir hasn't been deleted
        if !self.deleted.remove(&name) {
            // and it already exists
            if let Some(entry) = self.contents.get(&name) {
                // return the existing dir
                return match entry {
                    DirEntry::Dir(dir_lock) => Ok(dir_lock.clone()),
                    DirEntry::File(file) => Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        format!("there is already a file at {}: {:?}", name, file),
                    )),
                };
            }
        }

        let mut path = self.path.clone();
        path.push(&name);

        let lock = DirLock::new(self.cache.clone(), path);
        self.contents.insert(name, DirEntry::Dir(lock.clone()));
        Ok(lock)
    }

    /// Delete the entry with the given `name` from this [`Dir`].
    ///
    /// Returns `true` if the given `name` was present.
    ///
    /// References to sub-directories and files remain valid even after deleting their parent
    /// directory, so writing to a file, after deleting its parent directory will re-create the
    /// directory on the filesystem.
    ///
    /// Make sure to call `sync` to delete any contents on the filesystem if it's possible for
    /// an new entry with the same name to be created later.
    pub fn delete(&mut self, name: String) -> bool {
        let exists = self.contents.contains_key(&name);
        self.deleted.insert(name);
        exists
    }

    /// Get the entry with the given `name` from this [`Dir`].
    pub fn get<Q: Eq + Hash + ?Sized>(&self, name: &Q) -> Option<&DirEntry<FE>>
    where
        String: Borrow<Q>,
    {
        if self.deleted.contains(name.borrow()) {
            None
        } else {
            self.contents.get(name)
        }
    }

    /// Get the subdirectory with the given `name` from this [`Dir`], if present.
    ///
    /// Also returns `None` if the entry at `name` is a file.
    pub fn get_dir<Q: Eq + Hash + ?Sized>(&self, name: &Q) -> Option<&DirLock<FE>>
    where
        String: Borrow<Q>,
    {
        if self.deleted.contains(name.borrow()) {
            None
        } else {
            match self.contents.get(name) {
                Some(DirEntry::Dir(dir_lock)) => Some(dir_lock),
                _ => None,
            }
        }
    }

    /// Get the file with the given `name` from this [`Dir`], if present.
    ///
    /// Also returns `None` if the entry at `name` is a directory.
    pub fn get_file<Q: Eq + Hash + ?Sized>(&self, name: &Q) -> Option<FileLock<FE>>
    where
        String: Borrow<Q>,
    {
        if self.deleted.contains(name.borrow()) {
            None
        } else {
            match self.contents.get(name) {
                Some(DirEntry::File(file_lock)) => Some(file_lock.clone()),
                _ => None,
            }
        }
    }

    /// Return `true` if this [`Dir`] contains no entries.
    pub fn is_empty(&self) -> bool {
        if self.contents.is_empty() {
            true
        } else {
            self.contents
                .keys()
                .filter(|name| !self.deleted.contains(*name))
                .next()
                .is_none()
        }
    }

    /// Return an [`Iterator`] over the entries in this [`Dir`].
    pub fn iter(&self) -> impl Iterator<Item = (&String, &DirEntry<FE>)> {
        self.contents
            .iter()
            .filter(move |(name, _)| !self.deleted.contains(*name))
    }

    /// Return the number of entries in this [`Dir`].
    pub fn len(&self) -> usize {
        self.contents
            .keys()
            .filter(|name| !self.deleted.contains(*name))
            .count()
    }
}

impl<FE> fmt::Debug for Dir<FE> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "cached directory at {:?}", self.path)
    }
}

/// A clone-able wrapper type over a [`tokio::sync::RwLock`] on a directory.
pub struct DirLock<FE> {
    inner: Arc<RwLock<Dir<FE>>>,
}

impl<FE> Clone for DirLock<FE> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<FE> DirLock<FE> {
    fn new(cache: Arc<Cache<FE>>, path: PathBuf) -> Self {
        let dir = Dir {
            path,
            cache,
            contents: HashMap::new(),
            deleted: HashSet::new(),
        };

        Self {
            inner: Arc::new(RwLock::new(dir)),
        }
    }

    pub(crate) fn load<'a>(
        cache: Arc<Cache<FE>>,
        path: PathBuf,
    ) -> Pin<Box<dyn Future<Output = Result<Self, io::Error>> + 'a>>
    where
        FE: 'a,
    {
        Box::pin(async move {
            let mut contents = HashMap::new();
            let mut handles = fs::read_dir(&path).await?;

            while let Some(handle) = handles.next_entry().await? {
                let name = handle.file_name().into_string().map_err(|os_str| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("OS string is not a valid Rust string: {:?}", os_str),
                    )
                })?;

                let meta = handle.metadata().await?;
                if meta.is_dir() {
                    let subdirectory = Self::load(cache.clone(), handle.path()).await?;
                    contents.insert(name, DirEntry::Dir(subdirectory));
                } else if meta.is_file() {
                    let file = FileLock::load(cache.clone(), handle.path());
                    contents.insert(name, DirEntry::File(file));
                } else {
                    unreachable!("{:?} is neither a directory nor a file", handle.path());
                }
            }

            let dir = Dir {
                path,
                cache,
                contents,
                deleted: HashSet::new(),
            };

            let inner = Arc::new(RwLock::new(dir));
            Ok(DirLock { inner })
        })
    }

    /// Lock this directory for reading.
    pub async fn read(&self) -> DirReadGuard<FE> {
        let guard = self.inner.clone().read_owned().await;
        DirReadGuard { guard }
    }

    /// Lock this directory for reading synchronously, if possible.
    pub fn try_read(&self) -> Result<DirReadGuard<FE>, io::Error> {
        self.inner
            .clone()
            .try_read_owned()
            .map(|guard| DirReadGuard { guard })
            .map_err(|cause| io::Error::new(io::ErrorKind::WouldBlock, cause))
    }

    /// Lock this directory for writing.
    pub async fn write(&self) -> DirWriteGuard<FE> {
        let guard = self.inner.clone().write_owned().await;
        DirWriteGuard { guard }
    }

    /// Lock this directory for writing synchronously, if possible.
    pub fn try_write(&self) -> Result<DirWriteGuard<FE>, io::Error> {
        self.inner
            .clone()
            .try_write_owned()
            .map(|guard| DirWriteGuard { guard })
            .map_err(|cause| io::Error::new(io::ErrorKind::WouldBlock, cause))
    }

    /// Synchronize this directory with the filesystem.
    ///
    /// This will delete files and create subdirectories on the filesystem,
    /// but it will not synchronize any file contents.
    pub async fn sync(&self, err_if_locked: bool) -> Result<(), io::Error> {
        let mut dir = if err_if_locked {
            self.inner
                .try_write()
                .map_err(|cause| io::Error::new(io::ErrorKind::WouldBlock, cause))?
        } else {
            self.inner.write().await
        };

        let path = dir.path.clone();
        let deleted: Vec<String> = dir.deleted.drain().collect();
        let mut sync_deletes: FuturesUnordered<_> = deleted
            .into_iter()
            .filter_map(|name| dir.contents.remove(&name).map(|entry| (name, entry)))
            .filter_map(|(name, entry)| {
                let mut path = path.clone();
                path.push(name);

                if path.exists() {
                    Some(async move {
                        match entry {
                            DirEntry::Dir(_) => fs::remove_dir_all(path).await,
                            DirEntry::File(_) => fs::remove_file(path).await,
                        }
                    })
                } else {
                    None
                }
            })
            .collect();

        while let Some(()) = sync_deletes.try_next().await? {
            // nothing to do
        }

        let mut sync_creates: FuturesUnordered<_> = dir
            .contents
            .iter()
            .filter_map(|(name, entry)| {
                if entry.as_dir().is_some() {
                    let mut path = path.clone();
                    path.push(name);

                    return Some(async move {
                        while !path.exists() {
                            match fs::create_dir_all(&path).await {
                                Ok(()) => return Ok(()),
                                Err(cause) if cause.kind() == io::ErrorKind::AlreadyExists => {}
                                Err(cause) => return Err(cause),
                            }
                        }

                        Ok(())
                    });
                }

                None
            })
            .collect();

        while let Some(()) = sync_creates.try_next().await? {
            // nothing to do
        }

        Ok(())
    }

    /// Recursively delete empty entries in this [`Dir`].
    /// Returns the number of entries in this [`Dir`].
    /// Call this function immediately after loading the cache to avoid the risk of deadlock.
    pub fn trim(&self) -> Pin<Box<dyn Future<Output = usize> + '_>> {
        Box::pin(async move {
            let mut entries = self.write().await;

            let sizes = FuturesUnordered::new();
            for (name, entry) in entries.iter() {
                match entry {
                    DirEntry::Dir(dir) => sizes.push(dir.trim().map(|size| (name.clone(), size))),
                    DirEntry::File(_) => {}
                }
            }

            let sizes = sizes.collect::<Vec<_>>().await;
            for (name, size) in sizes {
                if size == 0 {
                    entries.delete(name);
                }
            }

            entries.len()
        })
    }
}

/// A read lock on a directory.
pub struct DirReadGuard<FE> {
    guard: OwnedRwLockReadGuard<Dir<FE>>,
}

impl<FE> Deref for DirReadGuard<FE> {
    type Target = Dir<FE>;

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

/// A write lock on a directory.
pub struct DirWriteGuard<FE> {
    guard: OwnedRwLockWriteGuard<Dir<FE>>,
}

impl<FE> Deref for DirWriteGuard<FE> {
    type Target = Dir<FE>;

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

impl<FE> DerefMut for DirWriteGuard<FE> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.deref_mut()
    }
}
