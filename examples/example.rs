use std::io;
use std::path::PathBuf;
use std::time::Duration;

use destream::en;
use rand::Rng;
use safecast::as_type;
use tokio::fs;
use tokio::io::AsyncWriteExt;

use freqfs::*;

enum File {
    Bin(Vec<u8>),
    Text(String),
}

impl<'en> en::ToStream<'en> for File {
    fn to_stream<E: en::Encoder<'en>>(&'en self, encoder: E) -> Result<E::Ok, E::Error> {
        match self {
            Self::Bin(bytes) => bytes.to_stream(encoder),
            Self::Text(string) => string.to_stream(encoder),
        }
    }
}

as_type!(File, Bin, Vec<u8>);
as_type!(File, Text, String);

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
            file.write_all(b"\"Hello, world!\"").await?;

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
            let mut contents: FileWriteGuard<String> = text_file.write().await?;
            assert_eq!(&*contents, "Hello, world!");

            // then they can be mutated purely in-memory
            *contents = "नमस्ते दुनिया!".to_string();
        }

        {
            let contents: FileReadGuard<String> = text_file.read().await?;
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
        let binary_file: FileReadGuard<Vec<u8>> = binary_file.read().await?;

        // now the cache is full, so the contents of "hello.txt" will be automatically sync'd
        // and removed from main memory
        tokio::time::sleep(Duration::from_millis(15)).await;

        // dropping "vector.bin" will allow its contents to be removed from the in-memory cache
        std::mem::drop(binary_file);

        // then loading "hello.txt" again will fill the cache
        let text_file = root.get_file("hello.txt").expect("text file");
        let contents: FileReadGuard<String> = text_file.read().await?;
        assert_eq!(&*contents, "नमस्ते दुनिया!");

        // so the contents of "vector.bin" will be automatically sync'd and removed from main memory
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // now it's safe to delete the entries whose locks have been dropped

    // deleting is a synchronous operation operation since it happens in-memory only
    assert!(root.delete("hello.txt").await);
    assert!(root.get("hello.txt").is_none());
    assert!(root.delete("subdir").await);
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
    let cache = Cache::new(40, None);

    // load the directory and file paths into memory (not file contents, yet)
    let root = cache.load(path.clone())?;

    // all I/O under the cache directory at `path` MUST now go through the cache methods
    // otherwise concurrent filesystem access may cause errors
    run_example(root).await?;

    let mut txt_file_path = path.clone();
    txt_file_path.push("hello.txt");
    assert!(!txt_file_path.exists());

    let mut sub_dir_path = path.clone();
    sub_dir_path.push("subdir");
    assert!(!sub_dir_path.exists());

    assert!(!path.exists());

    Ok(())
}
