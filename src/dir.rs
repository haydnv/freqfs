use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::io;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::{fmt, mem};

use futures::future::Future;
use log::warn;
use tokio::fs;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

use crate::file::FileLock;
use crate::{Cache, FileLoad};

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
    /// Returns `true` if there was an entry present.
    ///
    /// References to sub-directories and files remain valid even after deleting their parent
    /// directory, so writing to a file after deleting its parent directory will re-create the
    /// directory on the filesystem, and sync'ing the parent directory will delete the file.
    ///
    /// Make sure to call `sync` to delete any contents on the filesystem if it's possible for
    /// an new entry with the same name to be created later.
    pub fn delete(&mut self, name: String) -> Result<bool, io::Error> {
        if let Some(entry) = self.contents.get(&name) {
            self.deleted.insert(name);

            match entry {
                DirEntry::Dir(dir) => dir.delete_self()?,
                DirEntry::File(file) => file.delete(true)?,
            }

            Ok(true)
        } else {
            Ok(false)
        }
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

    /// Synchronize the contents of this directory with the filesystem.
    ///
    /// This will create new subdirectories and delete entries from the filesystem,
    /// but will NOT synchronize the contents of any child directories or files.
    pub fn sync(&mut self) -> Pin<Box<dyn Future<Output = Result<(), io::Error>> + '_>>
    where
        FE: FileLoad,
    {
        Box::pin(async move {
            let mut deleted = HashSet::new();
            mem::swap(&mut deleted, &mut self.deleted);
            for name in deleted {
                let entry = self.contents.remove(&name).expect("deleted dir entry");
                match entry {
                    DirEntry::Dir(subdir) => {
                        let mut subdir = subdir.write().await;
                        subdir.sync().await?;
                        fs::remove_dir_all(subdir.path()).await?;
                    }
                    DirEntry::File(file) => file.sync().await?,
                }
            }

            for entry in self.contents.values() {
                match entry {
                    DirEntry::Dir(dir) => dir.sync().await?,
                    DirEntry::File(file) => file.sync().await?,
                }
            }

            Ok(())
        })
    }

    fn delete_self(&mut self) -> Result<(), io::Error> {
        for (name, entry) in self.contents.iter() {
            self.deleted.insert(name.clone());

            match entry {
                DirEntry::Dir(dir) => dir.delete_self()?,
                DirEntry::File(file) => file.delete(false)?,
            }
        }

        Ok(())
    }
}

impl<FE> fmt::Debug for Dir<FE> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "cached directory at {:?}", self.path)
    }
}

/// A clone-able wrapper type over a [`tokio::sync::RwLock`] on a directory.
pub struct DirLock<FE> {
    state: Arc<RwLock<Dir<FE>>>,
}

impl<FE> Clone for DirLock<FE> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
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
            state: Arc::new(RwLock::new(dir)),
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
            Ok(DirLock { state: inner })
        })
    }

    /// Lock this directory for reading.
    pub async fn read(&self) -> DirReadGuard<FE> {
        let guard = self.state.clone().read_owned().await;
        DirReadGuard { guard }
    }

    /// Lock this directory for reading synchronously, if possible.
    pub fn try_read(&self) -> Result<DirReadGuard<FE>, io::Error> {
        self.state
            .clone()
            .try_read_owned()
            .map(|guard| DirReadGuard { guard })
            .map_err(|cause| io::Error::new(io::ErrorKind::WouldBlock, cause))
    }

    /// Lock this directory for writing.
    pub async fn write(&self) -> DirWriteGuard<FE> {
        let guard = self.state.clone().write_owned().await;
        DirWriteGuard { guard }
    }

    /// Lock this directory for writing synchronously, if possible.
    pub fn try_write(&self) -> Result<DirWriteGuard<FE>, io::Error> {
        self.state
            .clone()
            .try_write_owned()
            .map(|guard| DirWriteGuard { guard })
            .map_err(|cause| io::Error::new(io::ErrorKind::WouldBlock, cause))
    }

    /// Synchronize the contents of this directory with the filesystem.
    ///
    /// This will create new subdirectories and delete entries from the filesystem,
    /// but will NOT synchronize the contents of any child directories or files.
    pub fn sync(&self) -> Pin<Box<dyn Future<Output = Result<(), io::Error>> + '_>>
    where
        FE: FileLoad,
    {
        Box::pin(async move {
            let mut dir = self.state.write().await;
            dir.sync().await
        })
    }

    /// Recursively delete empty entries in this [`Dir`].
    /// Returns the number of entries in this [`Dir`].
    /// Call this function immediately after loading the cache to avoid the risk of deadlock.
    pub fn trim(&self) -> Result<usize, io::Error> {
        let mut entries = self
            .try_write()
            .map_err(|cause| io::Error::new(io::ErrorKind::WouldBlock, cause))?;

        let mut sizes = Vec::with_capacity(entries.len());
        for (name, entry) in entries.iter() {
            match entry {
                DirEntry::Dir(dir) => {
                    let size = dir.trim()?;
                    sizes.push((name.clone(), size));
                }
                DirEntry::File(_) => {}
            }
        }

        for (name, size) in sizes {
            if size == 0 {
                entries.delete(name)?;
            }
        }

        Ok(entries.len())
    }

    fn delete_self(&self) -> Result<(), io::Error> {
        let mut state = self.state.try_write().map_err(|cause| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("directory to delete is still in use: {}", cause),
            )
        })?;

        state.delete_self()
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
