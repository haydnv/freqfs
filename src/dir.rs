use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::io;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use futures::Future;
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

pub struct Dir<FE> {
    path: PathBuf,
    cache: Arc<Cache<FE>>,
    contents: HashMap<String, DirEntry<FE>>,
    deleted: HashSet<String>,
}

impl<FE> Dir<FE> {
    /// Create and return a new subdirectory of this `Dir`.
    pub fn create_dir(&mut self, name: String) -> Result<DirLock<FE>, io::Error> {
        if !self.deleted.remove(&name) {
            if self.contents.contains_key(&name) {
                return Err(io::Error::new(io::ErrorKind::AlreadyExists, name));
            }
        }

        let mut path = self.path.clone();
        path.push(&name);
        let lock = DirLock::new(self.cache.clone(), path);
        self.contents.insert(name, DirEntry::Dir(lock.clone()));
        Ok(lock)
    }

    /// Create a new file in this `Dir` with the given `contents`.
    pub fn create_file<F>(
        &mut self,
        name: String,
        contents: F,
        size_hint: Option<usize>,
    ) -> Result<FileLock<FE>, io::Error>
    where
        FE: From<F>,
    {
        if !self.deleted.remove(&name) {
            if self.contents.contains_key(&name) {
                return Err(io::Error::new(io::ErrorKind::AlreadyExists, name));
            }
        }

        let mut path = self.path.clone();
        path.push(&name);

        let size = size_hint.unwrap_or_default();
        let lock = FileLock::new(self.cache.clone(), path.clone(), contents, size);
        self.contents.insert(name, DirEntry::File(lock.clone()));
        self.cache.insert(path, lock.clone(), size);
        Ok(lock)
    }

    /// Delete the entry with the given `name` from this `Dir`.
    ///
    /// Returns `true` if the given `name` was present.
    pub fn delete<Q: AsRef<String>>(&mut self, name: Q) -> bool {
        self.deleted.insert(name.as_ref().to_owned());
        self.contents.contains_key(name.as_ref())
    }

    /// Get the entry with the given `name` from this `Dir`.
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

    /// Get the subdirectory with the given `name` from this `Dir`, if present.
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

    /// Get the file with the given `name` from this `Dir`, if present.
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

    /// Return an [`Iterator`] over the entries in this `Dir`.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &DirEntry<FE>)> {
        self.contents
            .iter()
            .filter(move |(name, _)| !self.deleted.contains(*name))
    }

    /// Return the number of entries in this `Dir`.
    pub fn len(&self) -> usize {
        self.contents
            .keys()
            .filter(|name| !self.deleted.contains(*name))
            .count()
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

    /// Lock this directory for writing.
    pub async fn write(&self) -> DirWriteGuard<FE> {
        let guard = self.inner.clone().write_owned().await;
        DirWriteGuard { guard }
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
