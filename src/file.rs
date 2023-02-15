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
pub trait FileLoad: Send + Sync + Sized + 'static {
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
    Modified(usize, Arc<RwLock<FE>>),
    Deleted(bool),
}

impl<FE> FileState<FE> {
    #[inline]
    fn is_pending(&self) -> bool {
        match self {
            Self::Pending | Self::Deleted(_) => true,
            _ => false,
        }
    }

    #[inline]
    fn upgrade(&mut self) {
        let (size, lock) = match self {
            Self::Pending | Self::Deleted(_) => panic!("cannot write a pending cache entry"),
            Self::Read(size, lock) => (*size, lock.clone()),
            Self::Modified(_size, _lock) => return,
        };

        *self = Self::Modified(size, lock);
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

    async fn get_lock(&self, mutate: bool) -> Result<Arc<RwLock<FE>>, io::Error>
    where
        FE: FileLoad,
    {
        let mut file_state = self.inner.contents.write().await;

        let newly_loaded = if file_state.is_pending() {
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

            if mutate {
                *file_state = FileState::Modified(size, Arc::new(RwLock::new(entry)));
            } else {
                *file_state = FileState::Read(size, Arc::new(RwLock::new(entry)));
            }

            true
        } else if mutate {
            file_state.upgrade();
            false
        } else {
            false
        };

        match &*file_state {
            FileState::Pending | FileState::Deleted(_) => unreachable!("read a pending file"),
            FileState::Read(size, contents) | FileState::Modified(size, contents) => {
                self.inner.cache.bump(&self.inner.path, *size, newly_loaded);
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

        if mutate && !file_state.is_pending() {
            file_state.upgrade();
        }

        match &*file_state {
            FileState::Pending | FileState::Deleted(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "file is not in cache",
                ))
            }
            FileState::Read(size, contents) | FileState::Modified(size, contents) => {
                self.inner.cache.bump(&self.inner.path, *size, false);
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
    pub async fn sync(&self) -> Result<(), io::Error>
    where
        FE: FileLoad,
    {
        let mut state = self.inner.contents.write().await;

        let new_state = match &*state {
            FileState::Modified(old_size, lock) => {
                let contents = lock.write().await;
                let new_size = persist(self.inner.path.as_path(), &*contents).await?;
                self.inner.cache.resize(*old_size, new_size as usize);
                FileState::Read(new_size as usize, lock.clone())
            }
            FileState::Deleted(file_only) => {
                if *file_only {
                    if self.path().exists() {
                        delete_file(self.path()).await?;
                    }
                }

                return Ok(());
            }
            _ => {
                // no-op
                return Ok(());
            }
        };

        *state = new_state;
        Ok(())
    }

    pub(crate) async fn delete(&self, file_only: bool) {
        let mut file_state = self.inner.contents.write().await;

        let size = match &*file_state {
            FileState::Pending => 0,
            FileState::Read(size, _) => *size,
            FileState::Modified(size, _) => *size,
            FileState::Deleted(_) => return,
        };

        self.inner.cache.remove(&self.inner.path, size);

        *file_state = FileState::Deleted(file_only);
    }

    pub(crate) async fn delete_and_sync(self) -> Result<(), io::Error> {
        let file_state = self.inner.contents.write().await;

        let size = match &*file_state {
            FileState::Pending => 0,
            FileState::Read(size, _) => *size,
            FileState::Modified(size, _) => *size,
            FileState::Deleted(_) => 0,
        };

        self.inner.cache.remove(&self.inner.path, size);

        if self.path().exists() {
            delete_file(self.path()).await
        } else {
            Ok(())
        }
    }

    pub(crate) fn evict(self) -> Option<(usize, impl Future<Output = Result<(), io::Error>>)>
    where
        FE: FileLoad + 'static,
    {
        // if this file is in use, don't evict it
        let mut state = self.inner.contents.clone().try_write_owned().ok()?;

        let (old_size, contents, modified) = match &*state {
            FileState::Pending => {
                // in this case there's nothing to evict
                return None;
            }
            FileState::Read(size, contents) => {
                let contents = contents.clone().try_write_owned().ok()?;
                (*size, contents, false)
            }
            FileState::Modified(size, contents) => {
                let contents = contents.clone().try_write_owned().ok()?;
                (*size, contents, true)
            }
            FileState::Deleted(_) => unreachable!("evict a deleted file"),
        };

        let eviction = async move {
            if modified {
                persist(self.inner.path.as_path(), &*contents).await?;
            }

            self.inner.cache.resize(old_size, 0);

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
            loop {
                match fs::File::create(tmp.as_path()).await {
                    Ok(file) => break file,
                    Err(cause) => match cause.kind() {
                        io::ErrorKind::NotFound => {
                            let parent = tmp.parent().expect("dir");
                            create_dir(parent).await?;
                        }
                        error_kind => {
                            return Err(io::Error::new(
                                error_kind,
                                format!("failed to create tmp file: {}", cause),
                            ))
                        }
                    },
                }
            }
        };

        let size = file.save(&mut tmp_file).await?;
        tmp_file.sync_all().await?;
        size
    };

    tokio::fs::rename(tmp.as_path(), path).await?;

    Ok(size)
}

async fn create_dir(path: &Path) -> Result<(), io::Error> {
    while !path.exists() {
        match tokio::fs::create_dir_all(path).await {
            Ok(()) => return Ok(()),
            Err(cause) if cause.kind() == io::ErrorKind::AlreadyExists => {
                // this just means there's another file in the same directory
                // being sync'd at the same time
            }
            Err(cause) => return Err(cause),
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

async fn delete_file(path: &Path) -> Result<(), io::Error> {
    match fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(cause) if cause.kind() == io::ErrorKind::NotFound => {
            // no-op
            Ok(())
        }
        Err(cause) => Err(cause),
    }
}
