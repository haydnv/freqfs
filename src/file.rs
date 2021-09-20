use std::io;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

use crate::Cache;

pub trait FileEntry<F> {
    fn as_file(&self) -> Option<&F>;

    fn as_file_mut(&mut self) -> Option<&mut F>;
}

pub trait File {}

enum FileState<FE> {
    Pending,
    Read(usize, Arc<RwLock<FE>>),
    Modified(usize, Arc<RwLock<FE>>),
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

    pub async fn read<F, E>(&self) -> Option<FileReadGuard<F>>
    where
        FE: FileEntry<F>,
    {
        unimplemented!()
    }

    pub async fn write<F, E>(&self) -> Option<FileWriteGuard<F>>
    where
        FE: FileEntry<F>,
    {
        unimplemented!()
    }
}

pub struct FileReadGuard<F> {
    guard: OwnedRwLockReadGuard<F>,
}

impl<F> Deref for FileReadGuard<F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

pub struct FileWriteGuard<F> {
    guard: OwnedRwLockWriteGuard<F>,
}

impl<F> FileWriteGuard<F> {
    pub fn sync(&self) -> Result<usize, io::Error> {
        unimplemented!()
    }
}

impl<F> Deref for FileWriteGuard<F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

impl<F> DerefMut for FileWriteGuard<F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.deref_mut()
    }
}
