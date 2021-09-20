use std::io;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use rand::Rng;
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
}

impl FileEntry<Vec<u8>> for File {
    fn as_file(&self) -> Option<&Vec<u8>> {
        match self {
            Self::Bin(bytes) => Some(bytes),
            _ => None,
        }
    }

    fn as_file_mut(&mut self) -> Option<&mut Vec<u8>> {
        match self {
            Self::Bin(bytes) => Some(bytes),
            _ => None,
        }
    }

    fn expected() -> &'static str {
        "a binary file"
    }
}

impl FileEntry<String> for File {
    fn as_file(&self) -> Option<&String> {
        match self {
            Self::Text(text) => Some(text),
            _ => None,
        }
    }

    fn as_file_mut(&mut self) -> Option<&mut String> {
        match self {
            Self::Text(text) => Some(text),
            _ => None,
        }
    }

    fn expected() -> &'static str {
        "a text file"
    }
}

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let mut rng = rand::thread_rng();
    let path = loop {
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

            break path;
        }
    };

    let root: DirLock<File> = load(path.clone(), 1000).await?;

    let lock = root.read().await;
    assert_eq!(lock.len(), 2);
    assert!(lock.get("hello").is_none());

    let file = lock.get_file("hello.txt").expect("file");
    let contents: FileReadGuard<File, String> = file.read().await?;
    assert_eq!(&*contents, "Hello, world!");

    tokio::fs::remove_dir_all(path).await
}
