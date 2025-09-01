use rayon::iter::IntoParallelRefIterator;


use rusqlite::{params, Connection, Result};
use walkdir::WalkDir;
use img_hash::image::io::Reader as ImageReader;
use img_hash::{HasherConfig, HashAlg};
use rayon::prelude::*;
use indicatif::{ProgressBar, ProgressStyle};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const DB_PATH: &str = "/home/whitepi/rust/dupcheckerrs/images.db";
const SEARCH_DIR: &str = "/media/whitepi/ATree";

fn main() -> Result<()> {
    let start = Instant::now();

    // Open or create the database
    let mut conn = Connection::open(DB_PATH)?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS hashes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash TEXT UNIQUE,
            path TEXT
        )",
        [],
    )?;

    // Collect all image file paths first
    let image_paths: Vec<_> = WalkDir::new(SEARCH_DIR)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|entry| {
            entry.path().is_file() && {
                let ext = entry.path().extension().and_then(|e| e.to_str()).unwrap_or("").to_lowercase();
                ext == "jpg" || ext == "jpeg"
            }
        })
        .map(|entry| entry.path().to_owned())
        .collect();

    let pb = ProgressBar::new(image_paths.len() as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}")
        .unwrap()
        .progress_chars("#>-"));

    // Use Arc<Mutex<>> to collect results from threads
    let results = Arc::new(Mutex::new(Vec::new()));
    let error_count = Arc::new(Mutex::new(0u64));

    image_paths.par_iter().for_each(|path| {
        let img_result = ImageReader::open(path)
            .and_then(|r| r.decode().map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)));
        match img_result {
            Ok(img) => {
                let hasher = HasherConfig::new().hash_alg(HashAlg::Mean).to_hasher();
                let hash = hasher.hash_image(&img);
                let hash_str = hash.to_base64();
                let path_str = path.to_string_lossy().to_string();
                results.lock().unwrap().push((hash_str, path_str));
            }
            Err(e) => {
                // Could not open image, print the path and error
                println!("Could not open image: {} (reason: {})", path.display(), e);
                let msg = e.to_string();
                if msg.contains("failed to fill whole buffer") || msg.contains("invalid JPEG format: first two bytes are not an SOI marker") || msg.contains("error") {
                    let _ = std::fs::remove_file(path);
                }
                *error_count.lock().unwrap() += 1;
            }
        }
        pb.inc(1);
    });
    pb.finish_with_message("Processing done");

    // Batch insert into DB
    let results = Arc::try_unwrap(results).unwrap().into_inner().unwrap();
    let tx = conn.transaction()?;
    for (hash_str, path_str) in results {
        let _ = tx.execute(
            "INSERT OR IGNORE INTO hashes (hash, path) VALUES (?1, ?2)",
            params![hash_str, path_str],
        );
    }
    tx.commit()?;

    let elapsed = start.elapsed();
    let total = image_paths.len();
    let errors = Arc::try_unwrap(error_count).unwrap().into_inner().unwrap();
    println!("Done. Elapsed time: {:.2?}", elapsed);
    println!("Total images processed: {}", total);
    println!("Total errors: {}", errors);

    // Move all unique images to RustMasterPics
    use std::fs;
    use std::path::Path;
    let dest_dir = "/media/whitepi/ATree/RustMasterPics";
    if !Path::new(dest_dir).exists() {
        if let Err(e) = fs::create_dir_all(dest_dir) {
            eprintln!("Failed to create {}: {}", dest_dir, e);
            return Ok(());
        }
    }
    let mut stmt = conn.prepare("SELECT path FROM hashes")?;
    let paths = stmt.query_map([], |row| row.get::<_, String>(0))?;
    for path_result in paths {
        if let Ok(path_str) = path_result {
            let src = Path::new(&path_str);
            if src.exists() {
                let filename = src.file_name().unwrap_or_default();
                let dest_path = Path::new(dest_dir).join(filename);
                if let Err(e) = fs::rename(src, &dest_path) {
                    eprintln!("Failed to move {} to {}: {}", src.display(), dest_path.display(), e);
                }
            }
        }
    }
    println!("All unique images moved to {}", dest_dir);
    Ok(())
}
