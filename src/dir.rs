use std::cmp::Ordering;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::{fmt, io};

use ds_ext::OrdHashMap;
use futures::future::{self, Future};
use futures::stream::{FuturesUnordered, StreamExt};
use log::warn;
use safecast::AsType;
use tokio::fs;
use tokio::sync::{
    OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock, RwLockReadGuard, RwLockWriteGuard,
};
use uuid::Uuid;

use super::cache::Cache;
use super::file::*;
use super::Result;

/// A read lock on a directory.
pub type DirReadGuard<'a, FE> = RwLockReadGuard<'a, Dir<FE>>;

/// An owned read lock on a directory.
pub type DirReadGuardOwned<FE> = OwnedRwLockReadGuard<Dir<FE>>;

/// A write lock on a directory.
pub type DirWriteGuard<'a, FE> = RwLockWriteGuard<'a, Dir<FE>>;

/// An owned write lock on a directory.
pub type DirWriteGuardOwned<FE> = OwnedRwLockWriteGuard<Dir<FE>>;

/// A type that can be used to look up a directory entry without calling `to_string()`,
/// to avoid unnecessary heap allocations.
pub trait Name {
    fn partial_cmp(&self, key: &String) -> Option<Ordering>;
}

impl Name for String {
    fn partial_cmp(&self, key: &String) -> Option<Ordering> {
        PartialOrd::partial_cmp(self, key)
    }
}

impl Name for str {
    fn partial_cmp(&self, key: &String) -> Option<Ordering> {
        PartialOrd::partial_cmp(self, key.as_str())
    }
}

impl Name for Arc<String> {
    fn partial_cmp(&self, key: &String) -> Option<Ordering> {
        PartialOrd::partial_cmp(&**self, key)
    }
}

#[macro_export]
macro_rules! name_from_str {
    ($t:ty) => {
        impl Name for $t {
            fn partial_cmp(&self, key: &String) -> Option<std::cmp::Ordering> {
                let key = key.parse().ok()?;
                std::cmp::PartialOrd::partial_cmp(self, &key)
            }
        }
    };
}

name_from_str!(u8);
name_from_str!(u16);
name_from_str!(u32);
name_from_str!(u64);
name_from_str!(u128);
name_from_str!(usize);
name_from_str!(i8);
name_from_str!(i16);
name_from_str!(i32);
name_from_str!(i64);
name_from_str!(i128);
name_from_str!(uuid::Uuid);

/// A directory entry, either a [`FileLock`] or a sub-[`DirLock`].
pub enum DirEntry<FE> {
    /// A subdirectory
    Dir(DirLock<FE>),

    /// A file in a directory
    File(FileLock<FE>),
}

impl<FE> Clone for DirEntry<FE> {
    fn clone(&self) -> Self {
        match self {
            Self::Dir(dir) => Self::Dir(dir.clone()),
            Self::File(file) => Self::File(file.clone()),
        }
    }
}

impl<FE> DirEntry<FE> {
    /// Return `Some(dir_lock)` if this [`DirEntry`] is itself a [`Dir`].
    pub fn as_dir(&self) -> Option<&DirLock<FE>> {
        match self {
            Self::Dir(dir) => Some(dir),
            _ => None,
        }
    }

    /// Return `Some(file_lock)` if this [`DirEntry`] is itself a file.
    pub fn as_file(&self) -> Option<&FileLock<FE>> {
        match self {
            Self::File(file) => Some(file),
            _ => None,
        }
    }

    /// Return `true` if this [`DirEntry`] is a [`Dir`].
    pub fn is_dir(&self) -> bool {
        match self {
            Self::Dir(_) => true,
            _ => false,
        }
    }

    /// Return `true` if this [`DirEntry`] is a file.
    pub fn is_file(&self) -> bool {
        match self {
            Self::Dir(_) => true,
            _ => false,
        }
    }
}

/// A filesystem directory
pub struct Dir<FE> {
    path: PathBuf,
    cache: Arc<Cache<FE>>,
    contents: OrdHashMap<String, DirEntry<FE>>,
    deleted: OrdHashMap<String, DirEntry<FE>>,
}

impl<FE: Send + Sync> Dir<FE> {
    /// Borrow the [`Path`] of this [`Dir`].
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    /// Return `true` if this [`Dir`] has an entry with the given `name`.
    pub fn contains<Q: Name + ?Sized>(&self, name: &Q) -> bool {
        if self.deleted.bisect(partial_cmp(name)).is_some() {
            false
        } else {
            self.contents.bisect(partial_cmp(name)).is_some()
        }
    }

    /// Create and return a new subdirectory of this [`Dir`].
    pub fn create_dir(&mut self, name: String) -> Result<DirLock<FE>> {
        if self.deleted.remove(&name).is_some() {
            warn!(
                "attempted to create a directory {} in {:?} that already exists",
                name, self.path
            );

            return Err(io::Error::new(io::ErrorKind::AlreadyExists, name));
        }

        let path = self.path.join(&name);

        let lock = DirLock::new(self.cache.clone(), path);

        self.contents.insert(name, DirEntry::Dir(lock.clone()));

        Ok(lock)
    }

    /// Create and return a new subdirectory of this [`Dir`] with a unique name.
    pub fn create_dir_unique(&mut self) -> Result<(Uuid, DirLock<FE>)> {
        let mut name = Uuid::new_v4();
        while self.contains(&name) {
            name = Uuid::new_v4();
        }

        let path = self.path.join(name.to_string());
        let lock = DirLock::new(self.cache.clone(), path);

        self.contents
            .insert(name.to_string(), DirEntry::Dir(lock.clone()));

        Ok((name, lock))
    }

    /// Return an [`Iterator`] over the entries in this [`Dir`].
    pub fn entries(&self) -> impl Iterator<Item = &DirEntry<FE>> {
        self.contents.values()
    }

    /// Return a new subdirectory of this [`Dir`], creating it if it doesn't already exist.
    pub fn get_or_create_dir(&mut self, name: String) -> Result<DirLock<FE>> {
        // if the requested dir hasn't been deleted
        if let Some(entry) = self.deleted.remove(&name) {
            // and it already exists
            // then return the existing dir
            return match entry {
                DirEntry::Dir(dir_lock) => Ok(dir_lock.clone()),
                DirEntry::File(file) => Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("there is already a file at {}: {:?}", name, file),
                )),
            };
        }

        let path = self.path.join(&name);

        let lock = DirLock::new(self.cache.clone(), path);
        self.contents.insert(name, DirEntry::Dir(lock.clone()));
        Ok(lock)
    }

    /// Get the entry with the given `name` from this [`Dir`].
    pub fn get<Q: Name + ?Sized>(&self, name: &Q) -> Option<&DirEntry<FE>> {
        if self.deleted.bisect(partial_cmp(name)).is_some() {
            None
        } else {
            self.contents.bisect(partial_cmp(name))
        }
    }

    /// Get the subdirectory with the given `name` from this [`Dir`], if present.
    ///
    /// Also returns `None` if the entry at `name` is a file.
    pub fn get_dir<Q: Name + ?Sized>(&self, name: &Q) -> Option<&DirLock<FE>> {
        if self.deleted.bisect(partial_cmp(name)).is_some() {
            None
        } else {
            match self.contents.bisect(partial_cmp(name)) {
                Some(DirEntry::Dir(dir_lock)) => Some(dir_lock),
                _ => None,
            }
        }
    }

    /// Get the file with the given `name` from this [`Dir`], if present.
    ///
    /// Also returns `None` if the entry at `name` is a directory.
    pub fn get_file<Q: Name + ?Sized>(&self, name: &Q) -> Option<&FileLock<FE>> {
        if self.deleted.bisect(partial_cmp(name)).is_some() {
            None
        } else {
            match self.contents.bisect(partial_cmp(name)) {
                Some(DirEntry::File(file_lock)) => Some(file_lock),
                _ => None,
            }
        }
    }

    /// Return `true` if this [`Dir`] contains no entries.
    pub fn is_empty(&self) -> bool {
        self.contents.is_empty()
    }

    /// Return an [`Iterator`] over the entries in this [`Dir`].
    pub fn iter(&self) -> impl Iterator<Item = (&String, &DirEntry<FE>)> {
        self.contents.iter()
    }

    /// Return the number of entries in this [`Dir`].
    pub fn len(&self) -> usize {
        self.contents.len()
    }

    /// Return an [`Iterator`] over the names of the entries in this [`Dir`].
    pub fn names(&self) -> impl Iterator<Item = &String> {
        self.contents.keys()
    }

    /// Convenience method to lock a file for reading.
    /// Returns a "not found" error if the there is no file with the given `name`.
    pub async fn read_file<Q, F>(&self, name: &Q) -> Result<FileReadGuard<F>>
    where
        Q: Name + fmt::Display + ?Sized,
        F: FileLoad,
        FE: AsType<F>,
    {
        if let Some(file) = self.get_file(name) {
            file.read().await
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, name.to_string()))
        }
    }

    /// Convenience method to lock a file for reading.
    /// Returns a "not found" error if the there is no file with the given `name`.
    pub async fn read_file_owned<Q, F>(&self, name: &Q) -> Result<FileReadGuardOwned<FE, F>>
    where
        Q: Name + fmt::Display + ?Sized,
        F: FileLoad,
        FE: AsType<F>,
    {
        if let Some(file) = self.get_file(name) {
            file.read_owned().await
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, name.to_string()))
        }
    }

    /// Convenience method to lock a file for writing.
    /// Returns a "not found" error if the there is no file with the given `name`.
    pub async fn write_file<Q, F>(&self, name: &Q) -> Result<FileWriteGuard<F>>
    where
        Q: Name + fmt::Display + ?Sized,
        F: FileLoad,
        FE: AsType<F>,
    {
        if let Some(file) = self.get_file(name) {
            file.write().await
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, name.to_string()))
        }
    }

    /// Convenience method to lock a file for writing.
    /// Returns a "not found" error if the there is no file with the given `name`.
    pub async fn write_file_owned<Q, F>(&self, name: &Q) -> Result<FileWriteGuardOwned<FE, F>>
    where
        Q: Name + fmt::Display + ?Sized,
        F: FileLoad,
        FE: AsType<F>,
    {
        if let Some(file) = self.get_file(name) {
            file.write_owned().await
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, name.to_string()))
        }
    }
}

impl<FE: Send + Sync> Dir<FE> {
    /// Create a new file in this [`Dir`] with the given `contents`.
    pub fn create_file<F>(&mut self, name: String, contents: F, size: usize) -> Result<FileLock<FE>>
    where
        FE: From<F>,
    {
        if self.deleted.remove(&name).is_some() {
            warn!(
                "attempted to create a file {} in {:?} that already exists",
                name, self.path
            );

            return Err(io::Error::new(io::ErrorKind::AlreadyExists, name));
        }

        let path = self.path.join(&name);

        let lock = FileLock::new(self.cache.clone(), path.clone(), contents, size);
        self.contents.insert(name, DirEntry::File(lock.clone()));
        self.cache.insert(path, lock.clone(), size);
        Ok(lock)
    }

    /// Create a new file in this [`Dir`] with a unique name and the given `contents`.
    pub fn create_file_unique<F>(
        &mut self,
        contents: F,
        size: usize,
    ) -> Result<(Uuid, FileLock<FE>)>
    where
        FE: From<F>,
    {
        let mut name = Uuid::new_v4();
        while self.contains(&name) {
            name = Uuid::new_v4();
        }

        self.create_file(name.to_string(), contents, size)
            .map(|file| (name, file))
    }

    /// Create a new file in this [`Dir`] by copying the given [`FileLock`],
    /// without reading its contents from the filesystem.
    pub async fn copy_file_from(&mut self, name: String, source: &FileLock<FE>) -> Result<()>
    where
        FE: Clone,
    {
        let path = self.path.join(&name);
        let lock = FileLock::load(self.cache.clone(), path.clone());

        lock.overwrite(source).await?; // this will update the size in the cache

        self.contents.insert(name, DirEntry::File(lock.clone()));
        self.cache.insert(path, lock, 0); // so just set the size to zero here

        Ok(())
    }

    /// Delete the entry with the given `name` from this [`Dir`].
    ///
    /// Returns `true` if there was an entry present.
    ///
    /// **This will cause a deadlock** if there are still active references to the deleted entry
    /// of this directory, i.e. if a lock cannot be acquired any child to delete (recursively)!
    ///
    /// Make sure to call [`Dir::sync`] to delete any contents on the filesystem if it's possible for
    /// an new entry with the same name to be created later.
    pub fn delete<'a, Q>(
        &'a mut self,
        name: &'a Q,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>>
    where
        Q: Name + Send + Sync + ?Sized,
    {
        Box::pin(async move {
            if let Some((name, entry)) = self.contents.bisect_and_remove(partial_cmp(name)) {
                match &entry {
                    DirEntry::Dir(dir) => dir.truncate().await,
                    DirEntry::File(file) => file.delete(true).await,
                }

                self.deleted.insert(name, entry);

                true
            } else {
                false
            }
        })
    }

    /// Synchronize the contents of this directory with the filesystem.
    ///
    /// This will create new subdirectories and delete entries from the filesystem,
    /// but will NOT synchronize the contents of any child directories or files.
    pub fn sync(&mut self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>
    where
        FE: for<'a> FileSave<'a>,
    {
        Box::pin(async move {
            if self.contents.is_empty() {
                self.deleted.clear();

                if self.path.exists() {
                    delete_dir(self.path()).await
                } else {
                    Ok(())
                }
            } else {
                for (_name, entry) in self.deleted.drain() {
                    match entry {
                        DirEntry::Dir(subdir) => {
                            let subdir = subdir.write().await;
                            delete_dir(subdir.path()).await?;
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
            }
        })
    }

    /// Delete all entries from this [`Dir`].
    ///
    /// **This will cause a deadlock** if there are still active references to the contents
    /// of this directory, i.e. if a lock cannot be acquired on any child of this [`Dir`]
    /// (recursively)!
    ///
    /// Make sure to call [`Dir::sync`] to delete any contents on the filesystem if it's possible
    /// for an new entry with the same name to be created later.
    /// Alternately, call [`Dir::truncate_and_sync`].
    pub fn truncate<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let mut deletions = FuturesUnordered::new();

            for (name, entry) in self.contents.drain() {
                deletions.push(async move {
                    match &entry {
                        DirEntry::Dir(dir) => dir.truncate().await,
                        DirEntry::File(file) => file.delete(false).await,
                    }

                    (name, entry)
                })
            }

            while let Some((name, entry)) = deletions.next().await {
                self.deleted.insert(name, entry);
            }
        })
    }

    /// Delete all entries from this [`Dir`] on the filesystem.
    ///
    /// **This will cause a deadlock** if there are still active references to the contents
    /// of this directory, i.e. if a lock cannot be acquired on any child of this [`Dir`]
    /// (recursively)!
    ///
    /// Make sure to call [`Dir::sync`] to delete any contents on the filesystem if it's possible
    /// for an new entry with the same name to be created later.
    /// Alternately, call [`Dir::truncate_and_sync`].
    pub fn truncate_and_sync<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let deletes = FuturesUnordered::new();

            for (_name, entry) in self.contents.drain() {
                deletes.push(async move {
                    match entry {
                        DirEntry::Dir(dir) => dir.truncate().await,
                        DirEntry::File(file) => file.delete(false).await,
                    }
                })
            }

            deletes.fold((), |(), ()| future::ready(())).await;
            delete_dir(self.path()).await
        })
    }
}

impl<FE> fmt::Debug for Dir<FE> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "cached directory at {:?}", self.path)
    }
}

/// A clone-able wrapper type over a [`RwLock`] on a directory.
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

impl<FE: Send + Sync> DirLock<FE> {
    fn new(cache: Arc<Cache<FE>>, path: PathBuf) -> Self {
        let dir = Dir {
            path,
            cache,
            contents: OrdHashMap::new(),
            deleted: OrdHashMap::new(),
        };

        Self {
            state: Arc::new(RwLock::new(dir)),
        }
    }

    // This doesn't need to be async since it's only called at initialization time
    pub(crate) fn load<'a>(cache: Arc<Cache<FE>>, path: PathBuf) -> Result<Self> {
        let mut contents = OrdHashMap::new();
        let mut handles = std::fs::read_dir(&path)?;

        while let Some(handle) = handles.next() {
            let handle = handle?;

            let name = handle.file_name().into_string().map_err(|os_str| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("OS string is not valid Unicode: {:?}", os_str),
                )
            })?;

            let meta = handle.metadata()?;
            if meta.is_dir() {
                let subdirectory = Self::load(cache.clone(), handle.path())?;
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
            deleted: OrdHashMap::new(),
        };

        let inner = Arc::new(RwLock::new(dir));
        Ok(DirLock { state: inner })
    }

    /// Lock this directory for reading.
    pub async fn read(&self) -> DirReadGuard<FE> {
        self.state.read().await
    }

    /// Lock this directory for reading synchronously, if possible.
    pub fn try_read(&self) -> Result<DirReadGuard<FE>> {
        self.state
            .try_read()
            .map_err(|cause| io::Error::new(io::ErrorKind::WouldBlock, cause))
    }

    /// Lock this directory for reading.
    pub async fn read_owned(&self) -> DirReadGuardOwned<FE> {
        self.state.clone().read_owned().await
    }

    /// Lock this directory for reading synchronously, if possible.
    pub fn try_read_owned(&self) -> Result<DirReadGuardOwned<FE>> {
        self.state
            .clone()
            .try_read_owned()
            .map_err(|cause| io::Error::new(io::ErrorKind::WouldBlock, cause))
    }

    /// Lock this directory for reading, without borrowing.
    pub async fn into_read(self) -> DirReadGuardOwned<FE> {
        self.state.read_owned().await
    }

    /// Lock this directory for writing.
    pub async fn write(&self) -> DirWriteGuard<FE> {
        self.state.write().await
    }

    /// Lock this directory for writing synchronously, if possible.
    pub fn try_write(&self) -> Result<DirWriteGuard<FE>> {
        self.state
            .try_write()
            .map_err(|cause| io::Error::new(io::ErrorKind::WouldBlock, cause))
    }

    /// Lock this directory for writing.
    pub async fn write_owned(&self) -> DirWriteGuardOwned<FE> {
        self.state.clone().write_owned().await
    }

    /// Lock this directory for writing synchronously, if possible.
    pub fn try_write_owned(&self) -> Result<DirWriteGuardOwned<FE>> {
        self.state
            .clone()
            .try_write_owned()
            .map_err(|cause| io::Error::new(io::ErrorKind::WouldBlock, cause))
    }

    /// Lock this directory for writing, without borrowing.
    pub async fn into_write(self) -> DirWriteGuardOwned<FE> {
        self.state.write_owned().await
    }

    /// Synchronize the contents of this directory with the filesystem.
    ///
    /// This will create new subdirectories and delete entries from the filesystem,
    /// but will NOT synchronize the contents of any child directories or files.
    pub fn sync(&self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>
    where
        FE: for<'a> FileSave<'a>,
    {
        Box::pin(async move {
            let mut dir = self.state.write().await;
            dir.sync().await
        })
    }

    /// Recursively delete empty entries in this [`Dir`].
    /// Returns the number of entries in this [`Dir`].
    /// Call this function immediately after loading the cache to avoid the risk of deadlock.
    pub fn trim(&self) -> Pin<Box<dyn Future<Output = Result<usize>> + '_>> {
        Box::pin(async move {
            let mut entries = self
                .try_write()
                .map_err(|cause| io::Error::new(io::ErrorKind::WouldBlock, cause))?;

            let mut sizes = Vec::with_capacity(entries.len());
            for (name, entry) in entries.iter() {
                match entry {
                    DirEntry::Dir(dir) => {
                        let size = dir.trim().await?;
                        sizes.push((name.clone(), size));
                    }
                    DirEntry::File(_) => {}
                }
            }

            for (name, size) in sizes {
                if size == 0 {
                    entries.delete(&name).await;
                }
            }

            Ok(entries.len())
        })
    }

    fn truncate(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let mut state = self.state.write().await;
            state.truncate().await
        })
    }
}

impl<FE> fmt::Debug for DirLock<FE> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("cached filesystem directory")
    }
}

async fn delete_dir(path: &Path) -> Result<()> {
    return match fs::remove_dir_all(path).await {
        Ok(()) => Ok(()),
        Err(cause) if cause.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(cause) => Err(cause),
    };
}

#[inline]
fn partial_cmp<'a, Q>(name: &'a Q) -> impl Fn(&String) -> Option<Ordering> + Copy + 'a
where
    Q: Name + ?Sized,
{
    |key| Name::partial_cmp(name, key)
}
