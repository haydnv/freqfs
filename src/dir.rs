use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::hash::Hash;
use std::io;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

use crate::file::{File, FileEntry, FileLock};
use crate::Cache;

#[derive(Clone)]
pub enum DirEntry<FE> {
    Dir(DirLock<FE>),
    File(FileLock<FE>),
}

impl<FE> DirEntry<FE> {
    pub fn as_dir(&self) -> Option<&DirLock<FE>> {
        match self {
            Self::Dir(dir) => Some(dir),
            _ => None,
        }
    }

    pub fn as_file(&self) -> Option<&FileLock<FE>> {
        match self {
            Self::File(file) => Some(file),
            _ => None,
        }
    }
}

pub struct Dir<FE> {
    path: PathBuf,
    cache: Arc<Cache>,
    contents: HashMap<String, DirEntry<FE>>,
    deleted: HashSet<String>,
}

impl<FE> Dir<FE> {
    pub fn create_dir<F, Q>(&mut self, name: Q) -> Result<DirLock<FE>, io::Error>
    where
        FE: From<F>,
        Q: AsRef<String>,
    {
        unimplemented!()
    }

    pub fn create_file<F, Q>(&mut self, name: Q, file: F) -> Result<FileLock<FE>, io::Error>
    where
        FE: From<F>,
        Q: AsRef<String>,
    {
        unimplemented!()
    }

    pub fn delete<Q: AsRef<String>>(&mut self, name: Q) -> bool {
        self.deleted.insert(name.as_ref().to_owned());
        self.contents.contains_key(name.as_ref())
    }

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
}

#[derive(Clone)]
pub struct DirLock<FE> {
    inner: Arc<RwLock<Dir<FE>>>,
}

impl<FE> DirLock<FE> {
    pub(crate) async fn load(_cache: Arc<Cache>, _path: PathBuf) -> Result<Self, io::Error> {
        unimplemented!()
    }

    pub async fn read(&self) -> DirReadGuard<FE> {
        unimplemented!()
    }

    pub async fn write(&self) -> DirReadGuard<FE> {
        unimplemented!()
    }
}

pub struct DirReadGuard<FE> {
    guard: OwnedRwLockReadGuard<Dir<FE>>,
}

impl<FE> Deref for DirReadGuard<FE> {
    type Target = Dir<FE>;

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

pub struct DirWriteGuard<FE> {
    guard: OwnedRwLockWriteGuard<Dir<FE>>,
}

impl<FE> DirWriteGuard<FE> {
    pub async fn sync(&self) -> Result<(), io::Error> {
        unimplemented!()
    }
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
