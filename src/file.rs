use std::convert::TryInto;
use std::fmt;
use std::io;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use futures::Future;
use safecast::AsType;
use tokio::fs;
use tokio::sync::{
    OwnedRwLockMappedWriteGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock,
};

use crate::Cache;

const TMP: &'static str = "_freqfs";

/// Load & save methods for a file data container type.
#[async_trait]
pub trait FileLoad: Sized {
    async fn load(
        path: &Path,
        file: fs::File,
        metadata: std::fs::Metadata,
    ) -> Result<Self, io::Error>;

    async fn save(&self, file: &mut fs::File) -> Result<u64, io::Error>;
}

enum FileState<FE> {
    Pending,
    Read(usize, Arc<RwLock<FE>>),
    #[allow(unused)]
    Modified(usize, Arc<RwLock<FE>>),
}

impl<FE> FileState<FE> {
    fn is_pending(&self) -> bool {
        match self {
            Self::Pending => true,
            _ => false,
        }
    }
}

struct Inner<FE> {
    cache: Arc<Cache<FE>>,
    path: PathBuf,
    contents: Arc<RwLock<FileState<FE>>>,
}

/// A clone-able wrapper over a [`tokio::sync::RwLock`] on a file container.
pub struct FileLock<FE> {
    inner: Arc<Inner<FE>>,
}

impl<FE> Clone for FileLock<FE> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<FE> FileLock<FE> {
    pub(crate) fn new<F>(cache: Arc<Cache<FE>>, path: PathBuf, file: F, size: usize) -> Self
    where
        FE: From<F>,
    {
        let lock = Arc::new(RwLock::new(file.into()));
        let state = FileState::Modified(size, lock);
        let inner = Inner {
            cache,
            path,
            contents: Arc::new(RwLock::new(state)),
        };

        Self {
            inner: Arc::new(inner),
        }
    }

    pub(crate) fn load(cache: Arc<Cache<FE>>, path: PathBuf) -> Self {
        let inner = Inner {
            cache,
            path,
            contents: Arc::new(RwLock::new(FileState::Pending)),
        };

        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn path(&self) -> &Path {
        self.inner.path.as_path()
    }

    pub async fn size_hint(&self) -> Option<usize> {
        let state = self.inner.contents.read().await;

        match &*state {
            FileState::Pending => None,
            FileState::Read(size, _) => Some(*size),
            FileState::Modified(size, _) => Some(*size),
        }
    }

    async fn get_lock(&self, mutate: bool) -> Result<Arc<RwLock<FE>>, io::Error>
    where
        FE: FileLoad,
    {
        let mut file_state = self.inner.contents.write().await;

        if file_state.is_pending() {
            let file = fs::File::open(&self.inner.path).await?;
            let metadata = file.metadata().await?;
            let size = match metadata.len().try_into() {
                Ok(size) => size,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::OutOfMemory,
                        format!(
                            "file at {:?} is too large to load into cache",
                            self.inner.path
                        ),
                    ))
                }
            };

            let entry = FE::load(self.inner.path.as_path(), file, metadata).await?;
            *file_state = FileState::Read(size, Arc::new(RwLock::new(entry)));
        }

        if mutate {
            let new_state = match &*file_state {
                FileState::Pending => unreachable!(),
                FileState::Read(size, lock) => FileState::Modified(*size, lock.clone()),
                FileState::Modified(size, lock) => FileState::Modified(*size, lock.clone()),
            };

            *file_state = new_state;
        }

        match &*file_state {
            FileState::Pending => unreachable!(),
            FileState::Read(size, contents) | FileState::Modified(size, contents) => {
                self.inner
                    .cache
                    .insert(self.inner.path.clone(), self.clone(), *size);

                Ok(contents.clone())
            }
        }
    }

    fn try_get_lock(&self, mutate: bool) -> Result<Arc<RwLock<FE>>, io::Error>
    where
        FE: FileLoad,
    {
        let mut file_state = self
            .inner
            .contents
            .try_write()
            .map_err(|cause| io::Error::new(io::ErrorKind::WouldBlock, cause))?;

        if mutate {
            let new_state = match &*file_state {
                FileState::Pending => {
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "file is not in cache",
                    ))
                }
                FileState::Read(size, lock) => FileState::Modified(*size, lock.clone()),
                FileState::Modified(size, lock) => FileState::Modified(*size, lock.clone()),
            };

            *file_state = new_state;
        }

        match &*file_state {
            FileState::Pending => {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "file is not in cache",
                ))
            }
            FileState::Read(size, contents) | FileState::Modified(size, contents) => {
                self.inner
                    .cache
                    .insert(self.inner.path.clone(), self.clone(), *size);

                Ok(contents.clone())
            }
        }
    }

    /// Lock this file for reading.
    pub async fn read<F>(&self) -> Result<FileReadGuard<FE, F>, io::Error>
    where
        FE: FileLoad + AsType<F>,
    {
        let contents = self.get_lock(false).await?;
        let guard = contents.read_owned().await;
        read_type(guard)
    }

    /// Lock this file for reading synchronously if possible, otherwise return an error.
    pub fn try_read<F>(&self) -> Result<FileReadGuard<FE, F>, io::Error>
    where
        FE: FileLoad + AsType<F>,
    {
        self.try_get_lock(false)
            .and_then(|contents| {
                contents
                    .try_read_owned()
                    .map_err(|cause| io::Error::new(io::ErrorKind::WouldBlock, cause))
            })
            .and_then(read_type)
    }

    /// Lock this file for writing.
    pub async fn write<F>(&self) -> Result<FileWriteGuard<FE, F>, io::Error>
    where
        FE: FileLoad + AsType<F>,
    {
        let contents = self.get_lock(true).await?;
        let guard = contents.write_owned().await;
        write_type(guard)
    }

    /// Lock this file for writing synchronously if possible, otherwise return an error.
    pub fn try_write<F>(&self) -> Result<FileWriteGuard<FE, F>, io::Error>
    where
        FE: FileLoad + AsType<F>,
    {
        self.try_get_lock(true)
            .and_then(|contents| {
                contents
                    .try_write_owned()
                    .map_err(|cause| io::Error::new(io::ErrorKind::WouldBlock, cause))
            })
            .and_then(write_type)
    }

    /// Back up this file's contents to the filesystem.
    ///
    /// This method acquires a write lock on the file contents, so it can deadlock
    /// if there is already a lock on the file contents.
    ///
    /// Pass `true` to error out if there is already a lock on the file contents.
    pub async fn sync(&self, err_if_locked: bool) -> Result<(), io::Error>
    where
        FE: FileLoad,
    {
        let mut state = self.inner.contents.write().await;

        let new_state = match &*state {
            FileState::Modified(old_size, lock) => {
                let contents = if err_if_locked {
                    lock.try_write()
                        .map_err(|cause| io::Error::new(io::ErrorKind::WouldBlock, cause))?
                } else {
                    lock.write().await
                };

                let new_size = persist(self.inner.path.as_path(), &*contents).await?;
                self.inner.cache.resize(*old_size, new_size as usize);
                FileState::Read(new_size as usize, lock.clone())
            }
            _ => return Ok(()), // no-op
        };

        *state = new_state;
        Ok(())
    }

    pub(crate) fn evict(self) -> Option<(usize, impl Future<Output = Result<(), io::Error>>)>
    where
        FE: FileLoad + 'static,
    {
        let mut state = self.inner.contents.clone().try_write_owned().ok()?;

        let (old_size, contents, modified) = match &*state {
            FileState::Pending => return None,
            FileState::Read(size, contents) => {
                let contents = contents.clone().try_write_owned().ok()?;
                (*size, contents, false)
            }
            FileState::Modified(size, contents) => {
                let contents = contents.clone().try_write_owned().ok()?;
                (*size, contents, true)
            }
        };

        let eviction = async move {
            if modified {
                persist(self.inner.path.as_path(), &*contents).await?;
            }

            let mut cache_state = self.inner.cache.inner.lock().expect("file cache state");
            if self.inner.cache.lfu.remove(&self.inner.path).is_some() {
                if old_size < cache_state.size {
                    cache_state.size -= old_size;
                } else {
                    cache_state.size = 0;
                }
            }

            *state = FileState::Pending;
            Ok(())
        };

        Some((old_size, eviction))
    }
}

impl<FE> fmt::Debug for FileLock<FE> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "cached file at {:?}", self.inner.path)
    }
}

/// A read lock on a file with data type `F`.
pub struct FileReadGuard<FE, F> {
    guard: OwnedRwLockReadGuard<FE, F>,
}

impl<FE, F> Deref for FileReadGuard<FE, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

/// A write lock on a file with data type `F`.
pub struct FileWriteGuard<FE, F> {
    guard: OwnedRwLockMappedWriteGuard<FE, F>,
}

impl<FE, F> Deref for FileWriteGuard<FE, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

impl<FE, F> DerefMut for FileWriteGuard<FE, F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.deref_mut()
    }
}

async fn persist<FE: FileLoad>(path: &Path, file: &FE) -> Result<u64, io::Error> {
    let tmp = if let Some(ext) = path.extension().and_then(|ext| ext.to_str()) {
        path.with_extension(format!("{}_{}", ext, TMP))
    } else {
        path.with_extension(TMP)
    };

    let size = {
        let mut tmp_file = if tmp.exists() {
            fs::OpenOptions::new()
                .truncate(true)
                .write(true)
                .open(tmp.as_path())
                .await?
        } else {
            create_parent(tmp.as_path()).await?;
            fs::File::create(tmp.as_path()).await?
        };

        let size = file.save(&mut tmp_file).await?;
        tmp_file.sync_all().await?;
        size
    };

    tokio::fs::rename(tmp.as_path(), path).await?;

    Ok(size)
}

async fn create_parent(path: &Path) -> Result<(), io::Error> {
    if let Some(parent) = path.parent() {
        while !parent.exists() {
            match tokio::fs::create_dir_all(parent).await {
                Ok(()) => return Ok(()),
                Err(cause) if cause.kind() == io::ErrorKind::AlreadyExists => {}
                Err(cause) => return Err(cause),
            }
        }
    }

    Ok(())
}

#[inline]
fn read_type<F, T>(guard: OwnedRwLockReadGuard<F>) -> Result<FileReadGuard<F, T>, io::Error>
where
    F: AsType<T>,
{
    OwnedRwLockReadGuard::try_map(guard, |entry| entry.as_type())
        .map(|guard| FileReadGuard { guard })
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid file type, expected {}", std::any::type_name::<F>()),
            )
        })
}

#[inline]
fn write_type<F, T>(guard: OwnedRwLockWriteGuard<F>) -> Result<FileWriteGuard<F, T>, io::Error>
where
    F: AsType<T>,
{
    OwnedRwLockWriteGuard::try_map(guard, |entry| entry.as_type_mut())
        .map(|guard| FileWriteGuard { guard })
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid file type, expected {}", std::any::type_name::<F>()),
            )
        })
}
