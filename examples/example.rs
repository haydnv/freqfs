use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use rand::Rng;
use safecast::AsType;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use freqfs::*;

enum File {
    Bin(Vec<u8>),
    Text(String),
}

#[async_trait]
impl FileLoad for File {
    async fn load(
        path: &Path,
        mut file: fs::File,
        metadata: std::fs::Metadata,
    ) -> Result<Self, io::Error> {
        match path.extension() {
            Some(ext) if ext.to_str() == Some("bin") => {
                let mut contents = Vec::with_capacity(metadata.len() as usize);
                file.read_to_end(&mut contents).await?;
                Ok(Self::Bin(contents))
            }
            Some(ext) if ext.to_str() == Some("txt") => {
                let mut contents = Vec::with_capacity(metadata.len() as usize);
                file.read_to_end(&mut contents).await?;
                String::from_utf8(contents)
                    .map(Self::Text)
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
            }
            other => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unrecognized extension: {:?}", other),
            )),
        }
    }

    async fn save(&self, file: &mut fs::File) -> Result<u64, io::Error> {
        match self {
            Self::Bin(bytes) => {
                file.write_all(bytes).await?;
                Ok(bytes.len() as u64)
            }
            Self::Text(text) => {
                let bytes = text.as_bytes();
                let size = bytes.len();
                file.write_all(bytes).await?;
                Ok(size as u64)
            }
        }
    }
}

impl AsType<Vec<u8>> for File {
    fn as_type(&self) -> Option<&Vec<u8>> {
        match self {
            Self::Bin(bytes) => Some(bytes),
            _ => None,
        }
    }

    fn as_type_mut(&mut self) -> Option<&mut Vec<u8>> {
        match self {
            Self::Bin(bytes) => Some(bytes),
            _ => None,
        }
    }

    fn into_type(self) -> Option<Vec<u8>> {
        match self {
            Self::Bin(bytes) => Some(bytes),
            _ => None,
        }
    }
}

impl AsType<String> for File {
    fn as_type(&self) -> Option<&String> {
        match self {
            Self::Text(text) => Some(text),
            _ => None,
        }
    }

    fn as_type_mut(&mut self) -> Option<&mut String> {
        match self {
            Self::Text(text) => Some(text),
            _ => None,
        }
    }

    fn into_type(self) -> Option<String> {
        match self {
            Self::Text(text) => Some(text),
            _ => None,
        }
    }
}

impl From<String> for File {
    fn from(text: String) -> Self {
        Self::Text(text)
    }
}

impl From<Vec<u8>> for File {
    fn from(bytes: Vec<u8>) -> Self {
        Self::Bin(bytes)
    }
}

async fn setup_tmp_dir() -> Result<PathBuf, io::Error> {
    let mut rng = rand::thread_rng();
    loop {
        let rand: u32 = rng.gen();
        let path = PathBuf::from(format!("/tmp/test_freqfs_{}", rand));
        if !path.exists() {
            fs::create_dir(&path).await?;

            let mut file_path = path.clone();
            file_path.push("hello.txt");

            let mut file = fs::File::create(file_path).await?;
            file.write_all(b"Hello, world!").await?;

            let mut subdir = path.clone();
            subdir.push("subdir");
            fs::create_dir(&subdir).await?;

            break Ok(path);
        }
    }
}

async fn run_example(cache: DirLock<File>) -> Result<(), io::Error> {
    let mut root = cache.write().await;

    assert_eq!(root.len(), 2);
    assert!(root.get("hello").is_none());

    {
        let text_file = root.get_file("hello.txt").expect("text file");

        {
            // to load a file into memory, acquire a lock on its contents
            let mut contents: FileWriteGuard<File, String> = text_file.write().await?;
            assert_eq!(&*contents, "Hello, world!");

            // then they can be mutated purely in-memory
            *contents = "नमस्ते दुनिया!".to_string();
        }

        {
            let contents: FileReadGuard<File, String> = text_file.read().await?;
            assert_eq!(&*contents, "नमस्ते दुनिया!");
        }

        // trigger an explicit sync of the new contents, without removing them from memory
        // this will update the cache with the new file size
        text_file.sync().await?;
    }

    {
        let mut sub_dir = root.get_dir("subdir").expect("subdirectory").write().await;

        // create a new directory
        // this is a synchronous operation since it happens in-memory only
        let sub_sub_dir = sub_dir.create_dir("sub-subdir".to_string())?;
        let mut sub_sub_dir = sub_sub_dir.write().await;

        // create a new file "vector.bin"
        // this is a synchronous operation since it happens in-memory only
        let binary_file =
            sub_sub_dir.create_file("vector.bin".to_string(), (0..25).collect::<Vec<u8>>(), 25)?;

        // then lock it so its data won't be evicted
        let binary_file: FileReadGuard<File, Vec<u8>> = binary_file.read().await?;

        // now the cache is full, so the contents of "hello.txt" will be automatically sync'd
        // and removed from main memory
        tokio::time::sleep(Duration::from_millis(15)).await;

        // dropping "vector.bin" will allow its contents to be removed from the in-memory cache
        std::mem::drop(binary_file);

        // then loading "hello.txt" again will fill the cache
        let text_file = root.get_file("hello.txt").expect("text file");
        let contents: FileReadGuard<File, String> = text_file.read().await?;
        assert_eq!(&*contents, "नमस्ते दुनिया!");

        // so the contents of "vector.bin" will be automatically sync'd and removed from main memory
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    // now it's safe to delete the entries whose locks have been dropped

    // deleting is a synchronous operation operation since it happens in-memory only
    assert!(root.delete("hello.txt".to_string()).expect("delete"));
    assert!(root.get("hello.txt").is_none());
    assert!(root.delete("subdir".to_string()).expect("delete"));
    assert!(root.get("subdir").is_none());

    // but we can explicitly sync to delete the file on the filesystem
    std::mem::drop(root); // make sure to drop the write lock first
    cache.sync().await?;

    // note that the delete happened even though `sub_dir` is still locked
    // so any writes to a file in `sub_dir` will re-create `sub_dir`

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let path = setup_tmp_dir().await?;

    // initialize the cache
    let cache = Cache::new(40, Duration::from_millis(10), None);

    // load the directory and file paths into memory (not file contents, yet)
    let root = cache.load(path.clone()).await?;

    // all I/O under the cache directory at `path` MUST now go through the cache methods
    // otherwise concurrent filesystem access may cause errors
    run_example(root).await?;

    let mut txt_file_path = path.clone();
    txt_file_path.push("hello.txt");
    assert!(!txt_file_path.exists());

    let mut sub_dir_path = path.clone();
    sub_dir_path.push("subdir");
    assert!(!sub_dir_path.exists());

    fs::remove_dir(path).await
}
