use rusqlite::{params, Connection, Result};
use walkdir::WalkDir;
use img_hash::image::io::Reader as ImageReader;
use img_hash::{HasherConfig, HashAlg};
use rayon::prelude::*;
use indicatif::{ProgressBar, ProgressStyle};
use std::io::{BufWriter, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use std::{fs, path::Path};

const DB_PATH: &str = "/media/PiTB/images.db";
const SEARCH_DIR: &str = "/media/PiTB/foofuck";
const DEST_DIR: &str = "/media/PiTB/RustMasterPics";
const CHUNK_SIZE: usize = 4096;
const ERROR_LOG_PATH: &str = "/media/PiTB/dupcheckerrs-errors.log";
const MAX_CONSOLE_ERRORS: u64 = 20;

enum ProcessResult {
    Hashed { hash: String, path: String },
    Error {
        path: String,
        message: String,
        deleted: bool,
    },
}

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

    // Optimize SQLite for bulk ingest on constrained devices.
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA temp_store = MEMORY;
         PRAGMA cache_size = -20000;",
    )?;

    // Track unique paths inserted during only this run.
    conn.execute(
        "CREATE TEMP TABLE IF NOT EXISTS run_inserted_paths (
            path TEXT PRIMARY KEY
        )",
        [],
    )?;
    conn.execute("DELETE FROM run_inserted_paths", [])?;

    // Collect all image file paths first to get deterministic progress length.
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

    let error_count = AtomicU64::new(0);
    let progress_count = AtomicU64::new(0);
    let progress_step = 32u64;
    let pb_worker = pb.clone();
    let mut total_inserted = 0u64;
    let mut console_errors_printed = 0u64;
    let mut console_errors_suppressed = 0u64;

    let mut error_log_writer = match fs::File::create(ERROR_LOG_PATH) {
        Ok(file) => Some(BufWriter::new(file)),
        Err(e) => {
            eprintln!(
                "Warning: failed to create error log {}: {}. Continuing without file logging.",
                ERROR_LOG_PATH, e
            );
            None
        }
    };

    for chunk in image_paths.chunks(CHUNK_SIZE) {
        // Build one hasher per worker thread and collect successful hashes without a global mutex.
        let results: Vec<ProcessResult> = chunk
            .par_iter()
            .map_init(
                || HasherConfig::new().hash_alg(HashAlg::Mean).to_hasher(),
                |hasher, path| {
                    let img_result = ImageReader::open(path).and_then(|r| {
                        r.decode()
                            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                    });

                    let output = match img_result {
                        Ok(img) => {
                            let hash = hasher.hash_image(&img);
                            let hash_str = hash.to_base64();
                            let path_str = path.to_string_lossy().to_string();
                            ProcessResult::Hashed {
                                hash: hash_str,
                                path: path_str,
                            }
                        }
                        Err(e) => {
                            let msg = e.to_string();
                            let mut deleted = false;
                            if msg.contains("failed to fill whole buffer")
                                || msg.contains(
                                    "invalid JPEG format: first two bytes are not an SOI marker",
                                )
                                || msg.contains("error")
                            {
                                deleted = std::fs::remove_file(path).is_ok();
                            }
                            error_count.fetch_add(1, Ordering::Relaxed);
                            ProcessResult::Error {
                                path: path.to_string_lossy().to_string(),
                                message: msg,
                                deleted,
                            }
                        }
                    };

                    let processed = progress_count.fetch_add(1, Ordering::Relaxed) + 1;
                    if processed % progress_step == 0 {
                        pb_worker.set_position(processed);
                    }
                    output
                },
            )
            .collect();

        // Insert each chunk before moving to the next one to keep memory bounded.
        let tx = conn.transaction()?;
        {
            let mut insert_hash_stmt =
                tx.prepare_cached("INSERT OR IGNORE INTO hashes (hash, path) VALUES (?1, ?2)")?;
            let mut insert_run_path_stmt = tx
                .prepare_cached("INSERT OR IGNORE INTO run_inserted_paths (path) VALUES (?1)")?;

            for result in results {
                match result {
                    ProcessResult::Hashed { hash, path } => {
                        let inserted = insert_hash_stmt.execute(params![hash, path.clone()])?;
                        if inserted > 0 {
                            insert_run_path_stmt.execute(params![path])?;
                            total_inserted += 1;
                        }
                    }
                    ProcessResult::Error {
                        path,
                        message,
                        deleted,
                    } => {
                        if console_errors_printed < MAX_CONSOLE_ERRORS {
                            eprintln!("Could not open image: {} (reason: {})", path, message);
                            console_errors_printed += 1;
                        } else {
                            console_errors_suppressed += 1;
                        }

                        if let Some(writer) = error_log_writer.as_mut() {
                            let _ = writeln!(
                                writer,
                                "path={}\treason={}\tdeleted={}",
                                path, message, deleted
                            );
                        }
                    }
                }
            }
        }
        tx.commit()?;
    }

    pb.set_position(image_paths.len() as u64);
    pb.finish_with_message("Processing done");

    let elapsed = start.elapsed();
    let total = image_paths.len();
    let errors = error_count.load(Ordering::Relaxed);
    println!("Done. Elapsed time: {:.2?}", elapsed);
    println!("Total images processed: {}", total);
    println!("Total errors: {}", errors);
    println!("Total unique hashes inserted this run: {}", total_inserted);

    if let Some(mut writer) = error_log_writer {
        let _ = writer.flush();
    }

    if console_errors_suppressed > 0 {
        println!(
            "Suppressed {} additional image error logs on console. Full diagnostics: {}",
            console_errors_suppressed, ERROR_LOG_PATH
        );
    }

    // Move all unique images inserted in this run after all hashing/inserts are complete.
    if !Path::new(DEST_DIR).exists() {
        if let Err(e) = fs::create_dir_all(DEST_DIR) {
            eprintln!("Failed to create {}: {}", DEST_DIR, e);
            return Ok(());
        }
    }

    let mut moved = 0u64;
    let mut move_stmt = conn.prepare("SELECT path FROM run_inserted_paths")?;
    let run_paths = move_stmt.query_map([], |row| row.get::<_, String>(0))?;
    for path_result in run_paths {
        if let Ok(path_str) = path_result {
            let src = Path::new(&path_str);
            if src.exists() {
                let filename = src.file_name().unwrap_or_default();
                let dest_path = Path::new(DEST_DIR).join(filename);
                if let Err(e) = fs::rename(src, &dest_path) {
                    eprintln!("Failed to move {} to {}: {}", src.display(), dest_path.display(), e);
                } else {
                    moved += 1;
                }
            }
        }
    }
    println!("Unique images moved this run: {}", moved);
    println!("Unique images from this run moved to {}", DEST_DIR);
    Ok(())
}
