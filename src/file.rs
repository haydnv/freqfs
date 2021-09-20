use std::convert::TryInto;
use std::io;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::fs;
use tokio::sync::{
    OwnedRwLockMappedWriteGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock,
};

use crate::Cache;

const TMP: &'static str = "_freqfs";

pub trait FileEntry<F> {
    fn expected() -> &'static str;

    fn as_file(&self) -> Option<&F>;

    fn as_file_mut(&mut self) -> Option<&mut F>;
}

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
    cache: Arc<Cache>,
    path: PathBuf,
    contents: RwLock<FileState<FE>>,
}

#[derive(Clone)]
pub struct FileLock<FE> {
    inner: Arc<Inner<FE>>,
}

impl<FE> FileLock<FE> {
    pub(crate) fn load(cache: Arc<Cache>, path: PathBuf) -> Self {
        let inner = Inner {
            cache,
            path,
            contents: RwLock::new(FileState::Pending),
        };

        Self {
            inner: Arc::new(inner),
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
                    .insert(self.inner.path.clone(), *size);

                Ok(contents.clone())
            }
        }
    }

    pub async fn read<F>(&self) -> Result<FileReadGuard<FE, F>, io::Error>
    where
        FE: FileLoad + FileEntry<F>,
    {
        let contents = self.get_lock().await?;
        let guard = contents.read_owned().await;
        OwnedRwLockReadGuard::try_map(guard, |entry| entry.as_file())
            .map(|guard| FileReadGuard { guard })
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid file type, expected {}", FE::expected()),
                )
            })
    }

    pub async fn write<F>(&self) -> Result<FileWriteGuard<FE, F>, io::Error>
    where
        FE: FileLoad + FileEntry<F>,
    {
        let contents = self.get_lock().await?;
        let guard = contents.write_owned().await;
        OwnedRwLockWriteGuard::try_map(guard, |entry| entry.as_file_mut())
            .map(|guard| FileWriteGuard { guard })
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid file type, expected {}", FE::expected()),
                )
            })
    }

    pub async fn sync(&self) -> Result<(), io::Error>
    where
        FE: FileLoad,
    {
        let mut state = self.inner.contents.write().await;
        let new_state = match &*state {
            FileState::Pending => return Ok(()), // no-op
            FileState::Read(_, lock) | FileState::Modified(_, lock) => {
                let contents = lock.write().await;
                let size = persist(self.inner.path.as_path(), &*contents).await?;
                FileState::Read(size as usize, lock.clone())
            }
        };

        *state = new_state;
        Ok(())
    }
}

pub struct FileReadGuard<FE, F> {
    guard: OwnedRwLockReadGuard<FE, F>,
}

impl<FE, F> Deref for FileReadGuard<FE, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

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
    let tmp = path.with_extension(TMP);

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
