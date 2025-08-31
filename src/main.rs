
use rusqlite::{params, Connection, Result};
use walkdir::WalkDir;
use img_hash::image::io::Reader as ImageReader;
use img_hash::{HasherConfig, HashAlg};

use std::fs;

use std::time::Instant;

const DB_PATH: &str = "/home/whitepi/rust/dupcheckerrs/images.db";
const SEARCH_DIR: &str = "/media/whitepi/ATree/";

fn main() -> Result<()> {
    let start = Instant::now();

    // Open or create the database
    let conn = Connection::open(DB_PATH)?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS hashes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash TEXT UNIQUE,
            path TEXT
        )",
        [],
    )?;

    for entry in WalkDir::new(SEARCH_DIR).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if path.is_file() {
            let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("").to_lowercase();
            if ext == "jpg" || ext == "jpeg" {
                println!("Processing: {}", path.display());
                let img_result = ImageReader::open(path)
                    .and_then(|r| r.decode().map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)));
                match img_result {
                    Ok(img) => {
                        let hasher = HasherConfig::new().hash_alg(HashAlg::Mean).to_hasher();
                        let hash = hasher.hash_image(&img);
                        let hash_str = hash.to_base64();
                        let path_str = path.to_string_lossy();
                        let _ = conn.execute(
                            "INSERT OR IGNORE INTO hashes (hash, path) VALUES (?1, ?2)",
                            params![hash_str, path_str],
                        );
                    }
                    Err(_) => {
                        // Could not open image, delete file
                        let _ = fs::remove_file(path);
                    }
                }
            }
        }
    }

    let elapsed = start.elapsed();
    println!("Done. Elapsed time: {:.2?}", elapsed);
    Ok(())
}
