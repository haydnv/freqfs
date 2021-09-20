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

#[allow(unused)]
pub struct Dir<FE> {
    path: PathBuf,
    cache: Arc<Cache>,
    contents: HashMap<String, DirEntry<FE>>,
    deleted: HashSet<String>,
}

impl<FE> Dir<FE> {
    pub fn create_dir<F, Q>(&mut self, _name: Q) -> Result<DirLock<FE>, io::Error>
    where
        FE: From<F>,
        Q: AsRef<String>,
    {
        unimplemented!()
    }

    pub fn create_file<F, Q>(&mut self, _name: Q, _file: F) -> Result<FileLock<FE>, io::Error>
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

    pub fn get_file<Q: Eq + Hash + ?Sized>(&self, name: &Q) -> Option<&FileLock<FE>>
    where
        String: Borrow<Q>,
    {
        if self.deleted.contains(name.borrow()) {
            None
        } else {
            match self.contents.get(name) {
                Some(DirEntry::File(file_lock)) => Some(file_lock),
                _ => None,
            }
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &DirEntry<FE>)> {
        self.contents
            .iter()
            .filter(move |(name, _)| !self.deleted.contains(*name))
    }

    pub fn len(&self) -> usize {
        self.contents
            .keys()
            .filter(|name| !self.deleted.contains(*name))
            .count()
    }
}

#[derive(Clone)]
pub struct DirLock<FE> {
    inner: Arc<RwLock<Dir<FE>>>,
}

impl<FE> DirLock<FE> {
    pub(crate) fn load(
        cache: Arc<Cache>,
        path: PathBuf,
    ) -> Pin<Box<dyn Future<Output = Result<Self, io::Error>>>> {
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

    pub async fn read(&self) -> DirReadGuard<FE> {
        let guard = self.inner.clone().read_owned().await;
        DirReadGuard { guard }
    }

    pub async fn write(&self) -> DirWriteGuard<FE> {
        let guard = self.inner.clone().write_owned().await;
        DirWriteGuard { guard }
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
