use std::convert::TryInto;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use futures::{Future, TryFutureExt};
use safecast::AsType;
use tokio::fs;
use tokio::sync::{
    OwnedRwLockMappedWriteGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock,
    RwLockMappedWriteGuard, RwLockReadGuard, RwLockWriteGuard,
};

use super::cache::Cache;

pub type FileReadGuard<'a, F> = RwLockReadGuard<'a, F>;
pub type FileReadGuardOwned<FE, F> = OwnedRwLockReadGuard<Option<FE>, F>;
pub type FileWriteGuard<'a, F> = RwLockMappedWriteGuard<'a, F>;
pub type FileWriteGuardOwned<FE, F> = OwnedRwLockMappedWriteGuard<Option<FE>, F>;

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

#[derive(Copy, Clone)]
enum FileLockState {
    Pending,
    Read(usize),
    Modified(usize),
    Deleted(bool),
}

impl FileLockState {
    fn is_deleted(&self) -> bool {
        match self {
            Self::Deleted(_) => true,
            _ => false,
        }
    }

    fn is_pending(&self) -> bool {
        match self {
            Self::Pending => true,
            _ => false,
        }
    }

    fn upgrade(&mut self) {
        let size = match self {
            Self::Read(size) | Self::Modified(size) => *size,
            _ => unreachable!("upgrade a file not in the cache"),
        };

        *self = Self::Modified(size);
    }
}

pub struct FileLock<FE> {
    cache: Arc<Cache<FE>>,
    path: Arc<PathBuf>,
    file: Arc<RwLock<Option<FE>>>,
    state: Arc<RwLock<FileLockState>>,
}

impl<FE> Clone for FileLock<FE> {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            path: self.path.clone(),
            file: self.file.clone(),
            state: self.state.clone(),
        }
    }
}

impl<FE> FileLock<FE> {
    /// Create a new [`FileLock`].
    pub fn new<F>(cache: Arc<Cache<FE>>, path: PathBuf, contents: F, size: usize) -> Self
    where
        FE: From<F>,
    {
        Self {
            cache,
            path: Arc::new(path),
            file: Arc::new(RwLock::new(Some(contents.into()))),
            state: Arc::new(RwLock::new(FileLockState::Modified(size))),
        }
    }

    /// Load a new [`FileLock`].
    pub fn load<F>(cache: Arc<Cache<FE>>, path: PathBuf) -> Self
    where
        FE: From<F>,
    {
        Self {
            cache,
            path: Arc::new(path),
            file: Arc::new(RwLock::new(None)),
            state: Arc::new(RwLock::new(FileLockState::Pending)),
        }
    }

    /// Lock this file for reading.
    pub async fn read<'a, F>(&'a self) -> Result<FileReadGuard<'a, F>, io::Error>
    where
        F: 'a,
        FE: FileLoad + AsType<F>,
    {
        if let Ok(maybe_file) = self.file.try_read() {
            if maybe_file.is_some() {
                self.cache.bump(&self.path, None);
                return read_type(maybe_file);
            }
        }

        let mut state = self.state.write().await;

        if state.is_deleted() {
            return Err(deleted());
        }

        let guard = if state.is_pending() {
            let mut contents = self.file.try_write().expect("file contents");
            let (size, entry) = load(&**self.path).await?;

            self.cache.bump(&self.path, Some(size));

            *state = FileLockState::Read(size);
            *contents = Some(entry);

            contents.downgrade()
        } else {
            self.cache.bump(&self.path, None);
            self.file.read().await
        };

        read_type(guard)
    }

    /// Lock this file for reading synchronously if possible, otherwise return an error.
    pub fn try_read<F>(&self) -> Result<FileReadGuard<F>, io::Error>
    where
        FE: FileLoad + AsType<F>,
    {
        let state = self.state.try_read().map_err(would_block)?;

        match &*state {
            FileLockState::Pending => Err(would_block("this file is not in the cache")),
            FileLockState::Deleted(_sync) => Err(deleted()),
            FileLockState::Read(_size) | FileLockState::Modified(_size) => {
                self.cache.bump(&self.path, None);
                let guard = self.file.try_read().map_err(would_block)?;
                read_type(guard)
            }
        }
    }

    /// Lock this file for reading.
    pub async fn read_owned<F>(&self) -> Result<FileReadGuardOwned<FE, F>, io::Error>
    where
        FE: FileLoad + AsType<F>,
    {
        if let Ok(maybe_file) = self.file.clone().try_read_owned() {
            if maybe_file.is_some() {
                self.cache.bump(&self.path, None);
                return read_type_owned(maybe_file);
            }
        }

        let mut state = self.state.write().await;

        if state.is_deleted() {
            return Err(deleted());
        }

        let guard = if state.is_pending() {
            let mut contents = self.file.clone().try_write_owned().expect("file contents");
            let (size, entry) = load(&**self.path).await?;

            self.cache.bump(&self.path, Some(size));

            *state = FileLockState::Read(size);
            *contents = Some(entry);

            contents.downgrade()
        } else {
            self.cache.bump(&self.path, None);
            self.file.clone().read_owned().await
        };

        read_type_owned(guard)
    }

    /// Lock this file for reading synchronously if possible, otherwise return an error.
    pub fn try_read_owned<F>(&self) -> Result<FileReadGuardOwned<FE, F>, io::Error>
    where
        FE: FileLoad + AsType<F>,
    {
        let state = self.state.try_read().map_err(would_block)?;

        match &*state {
            FileLockState::Pending => Err(would_block("this file is not in the cache")),
            FileLockState::Deleted(_sync) => Err(deleted()),
            FileLockState::Read(_size) | FileLockState::Modified(_size) => {
                self.cache.bump(&self.path, None);
                let guard = self.file.clone().try_read_owned().map_err(would_block)?;
                read_type_owned(guard)
            }
        }
    }

    /// Lock this file for reading, without borrowing.
    pub async fn into_read<F>(self) -> Result<FileReadGuardOwned<FE, F>, io::Error>
    where
        FE: FileLoad + AsType<F>,
    {
        let mut state = self.state.write().await;

        if state.is_deleted() {
            return Err(deleted());
        }

        let guard = if state.is_pending() {
            let mut contents = self.file.try_write_owned().expect("file contents");
            let (size, entry) = load(&**self.path).await?;

            self.cache.bump(&self.path, Some(size));

            *state = FileLockState::Read(size);
            *contents = Some(entry);

            contents.downgrade()
        } else {
            self.cache.bump(&self.path, None);
            self.file.read_owned().await
        };

        read_type_owned(guard)
    }

    /// Lock this file for writing.
    pub async fn write<'a, F>(&'a self) -> Result<FileWriteGuard<'a, F>, io::Error>
    where
        F: 'a,
        FE: FileLoad + AsType<F>,
    {
        if let Ok(maybe_file) = self.file.try_write() {
            if maybe_file.is_some() {
                self.cache.bump(&self.path, None);
                return write_type(maybe_file);
            }
        }

        let mut state = self.state.write().await;

        if state.is_deleted() {
            return Err(deleted());
        }

        let guard = if state.is_pending() {
            let mut contents = self.file.try_write().expect("file contents");
            let (size, entry) = load(&**self.path).await?;

            self.cache.bump(&self.path, Some(size));

            *state = FileLockState::Modified(size);
            *contents = Some(entry);

            self.cache.bump(&self.path, Some(size));

            contents
        } else {
            state.upgrade();
            self.cache.bump(&self.path, None);
            self.file.write().await
        };

        write_type(guard)
    }

    /// Lock this file for writing synchronously if possible, otherwise return an error.
    pub fn try_write<F>(&self) -> Result<FileWriteGuard<F>, io::Error>
    where
        FE: FileLoad + AsType<F>,
    {
        let mut state = self.state.try_write().map_err(would_block)?;

        if state.is_pending() {
            Err(would_block("this file is not in the cache"))
        } else if state.is_deleted() {
            Err(deleted())
        } else {
            state.upgrade();
            self.cache.bump(&self.path, None);
            let guard = self.file.try_write().map_err(would_block)?;
            write_type(guard)
        }
    }

    /// Lock this file for writing.
    pub async fn write_owned<F>(&self) -> Result<FileWriteGuardOwned<FE, F>, io::Error>
    where
        FE: FileLoad + AsType<F>,
    {
        if let Ok(maybe_file) = self.file.clone().try_write_owned() {
            if maybe_file.is_some() {
                self.cache.bump(&self.path, None);
                return write_type_owned(maybe_file);
            }
        }

        let mut state = self.state.write().await;

        if state.is_deleted() {
            return Err(deleted());
        }

        let guard = if state.is_pending() {
            let mut contents = self.file.clone().try_write_owned().expect("file contents");
            let (size, entry) = load(&**self.path).await?;
            self.cache.bump(&self.path, Some(size));

            *state = FileLockState::Modified(size);
            *contents = Some(entry);

            contents
        } else {
            state.upgrade();
            self.cache.bump(&self.path, None);
            self.file.clone().write_owned().await
        };

        write_type_owned(guard)
    }

    /// Lock this file for writing synchronously if possible, otherwise return an error.
    pub fn try_write_owned<F>(&self) -> Result<FileWriteGuardOwned<FE, F>, io::Error>
    where
        FE: FileLoad + AsType<F>,
    {
        let mut state = self.state.try_write().map_err(would_block)?;

        if state.is_pending() {
            Err(would_block("this file is not in the cache"))
        } else if state.is_deleted() {
            Err(deleted())
        } else {
            state.upgrade();
            self.cache.bump(&self.path, None);
            let guard = self.file.clone().try_write_owned().map_err(would_block)?;
            write_type_owned(guard)
        }
    }

    /// Lock this file for writing, without borrowing.
    pub async fn into_write<F>(self) -> Result<FileWriteGuardOwned<FE, F>, io::Error>
    where
        FE: FileLoad + AsType<F>,
    {
        let mut state = self.state.write().await;

        if state.is_deleted() {
            return Err(deleted());
        }

        let guard = if state.is_pending() {
            let mut contents = self.file.try_write_owned().expect("file contents");
            let (size, entry) = load(&**self.path).await?;
            self.cache.bump(&self.path, Some(size));

            *state = FileLockState::Modified(size);
            *contents = Some(entry);

            contents
        } else {
            state.upgrade();
            self.cache.bump(&self.path, None);
            self.file.write_owned().await
        };

        write_type_owned(guard)
    }

    /// Back up this file's contents to the filesystem.
    pub async fn sync(&self) -> Result<(), io::Error>
    where
        FE: FileLoad,
    {
        let mut state = self.state.write().await;

        let new_state = match &*state {
            FileLockState::Pending => FileLockState::Pending,
            FileLockState::Read(size) => FileLockState::Read(*size),
            FileLockState::Modified(old_size) => {
                let contents = self.file.read().await;
                let contents = contents.as_ref().expect("file");

                let new_size = persist(self.path.as_path(), contents).await?;

                self.cache.resize(*old_size, new_size as usize);
                FileLockState::Read(new_size as usize)
            }
            FileLockState::Deleted(needs_sync) => {
                if *needs_sync {
                    if self.path.exists() {
                        delete_file(&self.path).await?;
                    }
                }

                FileLockState::Deleted(false)
            }
        };

        *state = new_state;

        Ok(())
    }

    pub(crate) async fn delete(&self, file_only: bool) {
        let mut file_state = self.state.write().await;

        let size = match &*file_state {
            FileLockState::Pending => 0,
            FileLockState::Read(size) => *size,
            FileLockState::Modified(size) => *size,
            FileLockState::Deleted(_) => return,
        };

        self.cache.remove(&self.path, size);

        *file_state = FileLockState::Deleted(file_only);
    }

    pub(crate) async fn delete_and_sync(self) -> Result<(), io::Error> {
        let file_state = self.state.write().await;

        let size = match &*file_state {
            FileLockState::Pending => 0,
            FileLockState::Read(size) => *size,
            FileLockState::Modified(size) => *size,
            FileLockState::Deleted(_) => 0,
        };

        self.cache.remove(&self.path, size);

        if self.path.exists() {
            delete_file(&self.path).await
        } else {
            Ok(())
        }
    }

    pub(crate) fn evict(self) -> Option<(usize, impl Future<Output = Result<(), io::Error>>)>
    where
        FE: FileLoad + 'static,
    {
        // if this file is in use, don't evict it
        let mut state = self.state.try_write_owned().ok()?;

        let (old_size, contents, modified) = match &*state {
            FileLockState::Pending => {
                // in this case there's nothing to evict
                return None;
            }
            FileLockState::Read(size) => {
                let contents = self.file.try_write_owned().ok()?;
                (*size, contents, false)
            }
            FileLockState::Modified(size) => {
                let contents = self.file.try_write_owned().ok()?;
                (*size, contents, true)
            }
            FileLockState::Deleted(_) => unreachable!("evict a deleted file"),
        };

        let eviction = async move {
            if modified {
                let contents = contents.as_ref().expect("file");
                persist(self.path.as_path(), contents).await?;
            }

            self.cache.resize(old_size, 0);

            *state = FileLockState::Pending;
            Ok(())
        };

        Some((old_size, eviction))
    }
}

impl<FE> fmt::Debug for FileLock<FE> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        #[cfg(debug_assertions)]
        write!(f, "file at {}", self.path.display())?;

        #[cfg(not(debug_assertions))]
        f.write_str("a file lock")?;

        Ok(())
    }
}

async fn load<FE: FileLoad>(path: &Path) -> Result<(usize, FE), io::Error> {
    let file = fs::File::open(path).await?;
    let metadata = file.metadata().await?;
    let size = match metadata.len().try_into() {
        Ok(size) => size,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "this file is too large to load into the cache",
            ))
        }
    };

    let entry = FE::load(path, file, metadata).await?;

    Ok((size, entry))
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
            let parent = tmp.parent().expect("dir");
            let mut i = 0;
            while !parent.exists() {
                create_dir(parent).await?;
                tokio::time::sleep(tokio::time::Duration::from_millis(i)).await;
                i += 1;
            }

            assert!(parent.exists());

            fs::File::create(tmp.as_path())
                .map_err(|cause| {
                    io::Error::new(
                        cause.kind(),
                        format!("failed to create tmp file: {}", cause),
                    )
                })
                .await?
        };

        assert!(tmp.exists());
        assert!(!tmp.is_dir());

        let size = file
            .save(&mut tmp_file)
            .map_err(|cause| {
                io::Error::new(cause.kind(), format!("failed to save tmp file: {}", cause))
            })
            .await?;

        tmp_file
            .sync_all()
            .map_err(|cause| {
                io::Error::new(cause.kind(), format!("failed to sync tmp file: {}", cause))
            })
            .await?;

        size
    };

    tokio::fs::rename(tmp.as_path(), path)
        .map_err(|cause| {
            io::Error::new(
                cause.kind(),
                format!("failed to rename tmp file: {}", cause),
            )
        })
        .await?;

    Ok(size)
}

async fn create_dir(path: &Path) -> Result<(), io::Error> {
    if path.exists() {
        Ok(())
    } else {
        match tokio::fs::create_dir_all(path).await {
            Ok(()) => Ok(()),
            Err(cause) => {
                if path.exists() && path.is_dir() {
                    Ok(())
                } else {
                    return Err(io::Error::new(
                        cause.kind(),
                        format!("failed to create directory: {}", cause),
                    ));
                }
            }
        }
    }
}

#[inline]
fn read_type<F, T>(maybe_file: RwLockReadGuard<Option<F>>) -> Result<RwLockReadGuard<T>, io::Error>
where
    F: AsType<T>,
{
    match RwLockReadGuard::try_map(maybe_file, |file| file.as_ref().expect("file").as_type()) {
        Ok(file) => Ok(file),
        Err(_) => Err(invalid_data(format!(
            "invalid file type, expected {}",
            std::any::type_name::<F>()
        ))),
    }
}

#[inline]
fn read_type_owned<F, T>(
    maybe_file: OwnedRwLockReadGuard<Option<F>>,
) -> Result<OwnedRwLockReadGuard<Option<F>, T>, io::Error>
where
    F: AsType<T>,
{
    match OwnedRwLockReadGuard::try_map(maybe_file, |file| file.as_ref().expect("file").as_type()) {
        Ok(file) => Ok(file),
        Err(_) => Err(invalid_data(format!(
            "invalid file type, expected {}",
            std::any::type_name::<F>()
        ))),
    }
}

#[inline]
fn write_type<F, T>(
    maybe_file: RwLockWriteGuard<Option<F>>,
) -> Result<RwLockMappedWriteGuard<T>, io::Error>
where
    F: AsType<T>,
{
    match RwLockWriteGuard::try_map(maybe_file, |file| {
        file.as_mut().expect("file").as_type_mut()
    }) {
        Ok(file) => Ok(file),
        Err(_) => Err(invalid_data(format!(
            "invalid file type, expected {}",
            std::any::type_name::<F>()
        ))),
    }
}

#[inline]
fn write_type_owned<F, T>(
    maybe_file: OwnedRwLockWriteGuard<Option<F>>,
) -> Result<OwnedRwLockMappedWriteGuard<Option<F>, T>, io::Error>
where
    F: AsType<T>,
{
    match OwnedRwLockWriteGuard::try_map(maybe_file, |file| {
        file.as_mut().expect("file").as_type_mut()
    }) {
        Ok(file) => Ok(file),
        Err(_) => Err(invalid_data(format!(
            "invalid file type, expected {}",
            std::any::type_name::<F>()
        ))),
    }
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

#[inline]
fn deleted() -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, "this file has been deleted")
}

#[inline]
fn invalid_data<E>(cause: E) -> io::Error
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    io::Error::new(io::ErrorKind::InvalidData, cause)
}

#[inline]
fn would_block<E>(cause: E) -> io::Error
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    io::Error::new(io::ErrorKind::WouldBlock, cause)
}
