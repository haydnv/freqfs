use std::convert::TryInto;
use std::io;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use futures::Future;
use tokio::fs;
use tokio::sync::{
    OwnedRwLockMappedWriteGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock,
};

use crate::Cache;

const TMP: &'static str = "_freqfs";

/// Conversion methods from a container type (such as an `enum`) and file data.
pub trait FileEntry<F> {
    fn expected() -> &'static str;

    fn as_file(&self) -> Option<&F>;

    fn as_file_mut(&mut self) -> Option<&mut F>;
}

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

    fn size(&self) -> Option<usize> {
        match self {
            Self::Pending => None,
            Self::Read(size, _) | Self::Modified(size, _) => Some(*size),
        }
    }

    fn maybe_lock_contents(&self) -> Option<OwnedRwLockWriteGuard<FE>> {
        match self {
            Self::Pending => None,
            Self::Read(_, lock) | Self::Modified(_, lock) => lock.clone().try_write_owned().ok(),
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

    pub(crate) fn size(&self) -> Option<usize> {
        let state = self.inner.contents.try_read().ok()?;
        match &*state {
            FileState::Pending => None,
            FileState::Read(size, _) => Some(*size),
            FileState::Modified(size, _) => Some(*size),
        }
    }

    async fn get_lock(&self) -> Result<Arc<RwLock<FE>>, io::Error>
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

        let file_state = file_state.downgrade();
        match &*file_state {
            FileState::Pending => unreachable!(),
            FileState::Read(size, contents) | FileState::Modified(size, contents) => {
                self.inner
                    .clone()
                    .cache
                    .insert(self.inner.path.clone(), self.clone(), *size);

                Ok(contents.clone())
            }
        }
    }

    /// Lock this file for reading.
    pub async fn read<F>(&self) -> Result<FileReadGuard<FE, F>, io::Error>
    where
        FE: FileLoad + FileEntry<F>,
    {
        let contents = self.get_lock().await?;
        let guard = contents.read_owned().await;
        OwnedRwLockReadGuard::try_map(guard, |entry| entry.as_file())
            .map(|guard| {
                let rc = self.inner.clone();
                FileReadGuard { rc, guard }
            })
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid file type, expected {}", FE::expected()),
                )
            })
    }

    /// Lock this file for writing.
    pub async fn write<F>(&self) -> Result<FileWriteGuard<FE, F>, io::Error>
    where
        FE: FileLoad + FileEntry<F>,
    {
        let contents = self.get_lock().await?;
        let guard = contents.write_owned().await;
        OwnedRwLockWriteGuard::try_map(guard, |entry| entry.as_file_mut())
            .map(|guard| {
                let rc = self.inner.clone();
                FileWriteGuard { rc, guard }
            })
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid file type, expected {}", FE::expected()),
                )
            })
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

    pub(crate) fn evict(self) -> Option<impl Future<Output = Result<(), io::Error>>>
    where
        FE: FileLoad + 'static,
    {
        let mut state = self.inner.contents.clone().try_write_owned().ok()?;

        let old_size = state.size()?;
        let contents = state.maybe_lock_contents()?;

        Some(async move {
            persist(self.inner.path.as_path(), &*contents).await?;
            self.inner.cache.remove(&self.inner.path, old_size);
            *state = FileState::Pending;
            Ok(())
        })
    }
}

/// A read lock on a file with data type `F`.
pub struct FileReadGuard<FE, F> {
    #[allow(unused)]
    rc: Arc<Inner<FE>>,
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
    #[allow(unused)]
    rc: Arc<Inner<FE>>,
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
        if !parent.exists() {
            return tokio::fs::create_dir_all(parent).await;
        }
    }

    Ok(())
}
