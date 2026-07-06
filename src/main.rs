use rusqlite::{params, Connection, Result};
use walkdir::WalkDir;
use img_hash::image::{self, DynamicImage, ImageOutputFormat};
use img_hash::{HashAlg, HasherConfig};
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use std::{env, fs, io};
use time::OffsetDateTime;

const DB_PATH: &str = "/media/PiTB/images.db";
const SEARCH_DIR: &str = "/media/PiTB/foofuck";
const CHUNK_SIZE: usize = 4096;
const ERROR_LOG_FILE: &str = "dupcheckerrs-errors.log";
const TRANSCODE_DIR_NAME: &str = "transcoded_jpg";
const QUARANTINE_DIR_NAME: &str = "quarantine";
const MAX_CONSOLE_ERRORS: u64 = 20;

#[derive(Clone, Copy)]
enum DetectedFormat {
    Jpeg,
    Png,
    Gif,
    Webp,
    Bmp,
    Tiff,
    OtherImage,
    Unknown,
}

impl DetectedFormat {
    fn as_str(self) -> &'static str {
        match self {
            DetectedFormat::Jpeg => "jpeg",
            DetectedFormat::Png => "png",
            DetectedFormat::Gif => "gif",
            DetectedFormat::Webp => "webp",
            DetectedFormat::Bmp => "bmp",
            DetectedFormat::Tiff => "tiff",
            DetectedFormat::OtherImage => "other-image",
            DetectedFormat::Unknown => "unknown",
        }
    }

    fn is_transcode_candidate(self) -> bool {
        matches!(
            self,
            DetectedFormat::Png
                | DetectedFormat::Gif
                | DetectedFormat::Webp
                | DetectedFormat::Bmp
                | DetectedFormat::Tiff
                | DetectedFormat::OtherImage
        )
    }
}

enum ProcessResult {
    Hashed {
        hash: String,
        original_path: String,
        stored_path: String,
        detected_format: String,
        original_extension: String,
        action_taken: String,
        quarantine_path: Option<String>,
        transcode_path: Option<String>,
        ingest_ts: String,
    },
    Quarantined {
        path: String,
        detected_format: String,
        message: String,
        quarantine_path: String,
    },
    Error {
        path: String,
        message: String,
    },
}

fn extension_of(path: &Path) -> String {
    path.extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase()
}

fn detect_format(bytes: &[u8]) -> DetectedFormat {
    match image::guess_format(bytes) {
        Ok(image::ImageFormat::Jpeg) => DetectedFormat::Jpeg,
        Ok(image::ImageFormat::Png) => DetectedFormat::Png,
        Ok(image::ImageFormat::Gif) => DetectedFormat::Gif,
        Ok(image::ImageFormat::WebP) => DetectedFormat::Webp,
        Ok(image::ImageFormat::Bmp) => DetectedFormat::Bmp,
        Ok(image::ImageFormat::Tiff) => DetectedFormat::Tiff,
        Ok(_) => DetectedFormat::OtherImage,
        Err(_) => DetectedFormat::Unknown,
    }
}

fn path_fingerprint(path: &Path) -> u64 {
    let mut hasher = DefaultHasher::new();
    path.to_string_lossy().hash(&mut hasher);
    hasher.finish()
}

fn sanitize_stem(path: &Path) -> String {
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("image")
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect::<String>();
    if stem.is_empty() {
        "image".to_string()
    } else {
        stem
    }
}

fn unique_target_path(base_dir: &Path, source: &Path, extension: &str) -> PathBuf {
    let stem = sanitize_stem(source);
    let id = path_fingerprint(source);
    let mut candidate = base_dir.join(format!("{}_{}.{}", stem, id, extension));
    if !candidate.exists() {
        return candidate;
    }

    let mut idx = 1u32;
    loop {
        candidate = base_dir.join(format!("{}_{}_{}.{}", stem, id, idx, extension));
        if !candidate.exists() {
            return candidate;
        }
        idx += 1;
    }
}

fn transcode_to_jpeg(img: &DynamicImage, source: &Path, transcode_dir: &Path) -> io::Result<PathBuf> {
    let out_path = unique_target_path(transcode_dir, source, "jpg");
    let file = fs::File::create(&out_path)?;
    let mut writer = BufWriter::new(file);
    img.write_to(&mut writer, ImageOutputFormat::Jpeg(90))
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("jpeg encode failed: {}", e)))?;
    writer.flush()?;
    Ok(out_path)
}

fn quarantine_file(source: &Path, quarantine_dir: &Path) -> io::Result<PathBuf> {
    let ext = source
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("bin")
        .to_lowercase();
    let out_path = unique_target_path(quarantine_dir, source, &ext);

    match fs::rename(source, &out_path) {
        Ok(_) => Ok(out_path),
        Err(rename_err) => {
            if rename_err.raw_os_error() == Some(18) {
                fs::copy(source, &out_path)?;
                fs::remove_file(source)?;
                Ok(out_path)
            } else {
                Err(rename_err)
            }
        }
    }
}

fn env_flag(name: &str) -> bool {
    env::var(name)
        .ok()
        .map(|v| {
            let normalized = v.trim().to_ascii_lowercase();
            normalized == "1" || normalized == "true" || normalized == "yes" || normalized == "on"
        })
        .unwrap_or(false)
}

fn main() -> Result<()> {
    let start = Instant::now();
    let dry_run = env_flag("DUPCHECKERRS_DRY_RUN");
    let cwd = env::current_dir().expect("failed to read current working directory");
    let transcode_dir = cwd.join(TRANSCODE_DIR_NAME);
    let quarantine_dir = cwd.join(QUARANTINE_DIR_NAME);
    let error_log_path = cwd.join(ERROR_LOG_FILE);

    if dry_run {
        println!(
            "Dry run enabled (DUPCHECKERRS_DRY_RUN): previewing actions without DB writes, transcodes, or file moves"
        );
    }

    if !dry_run {
        if let Err(e) = fs::create_dir_all(&transcode_dir) {
            eprintln!(
                "Failed to create transcode output directory {}: {}",
                transcode_dir.display(),
                e
            );
            return Ok(());
        }
        if let Err(e) = fs::create_dir_all(&quarantine_dir) {
            eprintln!(
                "Failed to create quarantine directory {}: {}",
                quarantine_dir.display(),
                e
            );
            return Ok(());
        }
    }

    rayon::ThreadPoolBuilder::new()
        .num_threads(3)
        .build_global()
        .expect("failed to configure rayon thread pool");

    // Open and initialize the database only when not in dry-run mode.
    let mut conn = if dry_run {
        None
    } else {
        let conn = Connection::open(DB_PATH)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS hashes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hash TEXT UNIQUE,
                original_path TEXT NOT NULL,
                stored_path TEXT NOT NULL,
                detected_format TEXT NOT NULL,
                original_extension TEXT NOT NULL,
                action_taken TEXT NOT NULL,
                quarantine_path TEXT,
                transcode_path TEXT,
                ingest_ts TEXT NOT NULL
            )",
            [],
        )?;

        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA temp_store = MEMORY;
             PRAGMA cache_size = -20000;",
        )?;
        Some(conn)
    };

    // Keep extension-based discovery, then classify by magic bytes before decode.
    let image_paths: Vec<_> = WalkDir::new(SEARCH_DIR)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|entry| {
            entry.path().is_file() && {
                let ext = extension_of(entry.path());
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

    let mut quarantined_total = 0u64;
    let mut transcoded_total = 0u64;

    let mut error_log_writer = if dry_run {
        None
    } else {
        match fs::File::create(&error_log_path) {
            Ok(file) => Some(BufWriter::new(file)),
            Err(e) => {
                eprintln!(
                    "Warning: failed to create error log {}: {}. Continuing without file logging.",
                    error_log_path.display(),
                    e
                );
                None
            }
        }
    };

    for chunk in image_paths.chunks(CHUNK_SIZE) {
        // Build one hasher per worker thread and collect successful hashes without a global mutex.
        let results: Vec<ProcessResult> = chunk
            .par_iter()
            .map_init(
                || HasherConfig::new().hash_alg(HashAlg::Mean).to_hasher(),
                |hasher, path| {
                    let original_path = path.to_string_lossy().to_string();
                    let original_extension = extension_of(path);
                    let bytes = match fs::read(path) {
                        Ok(b) => b,
                        Err(e) => {
                            error_count.fetch_add(1, Ordering::Relaxed);
                            let output = ProcessResult::Error {
                                path: original_path,
                                message: format!("failed to read file: {}", e),
                            };
                            let processed = progress_count.fetch_add(1, Ordering::Relaxed) + 1;
                            if processed % progress_step == 0 {
                                pb_worker.set_position(processed);
                            }
                            return output;
                        }
                    };

                    let detected = detect_format(&bytes);

                    let output = match detected {
                        DetectedFormat::Unknown => match if dry_run {
                            let ext = path
                                .extension()
                                .and_then(|e| e.to_str())
                                .unwrap_or("bin")
                                .to_lowercase();
                            Ok(unique_target_path(&quarantine_dir, path, &ext))
                        } else {
                            quarantine_file(path, &quarantine_dir)
                        } {
                            Ok(quarantined) => {
                                error_count.fetch_add(1, Ordering::Relaxed);
                                ProcessResult::Quarantined {
                                    path: original_path,
                                    detected_format: detected.as_str().to_string(),
                                    message: if dry_run {
                                        "would quarantine unknown file signature".to_string()
                                    } else {
                                        "unknown file signature".to_string()
                                    },
                                    quarantine_path: quarantined.to_string_lossy().to_string(),
                                }
                            }
                            Err(e) => {
                                error_count.fetch_add(1, Ordering::Relaxed);
                                ProcessResult::Error {
                                    path: original_path,
                                    message: format!(
                                        "unknown file signature and quarantine move failed: {}",
                                        e
                                    ),
                                }
                            }
                        },
                        _ => match image::load_from_memory(&bytes) {
                            Ok(img) => {
                                let (stored_path, action_taken, transcode_path) = if matches!(detected, DetectedFormat::Jpeg) {
                                    (original_path.clone(), "kept_jpeg".to_string(), None)
                                } else if detected.is_transcode_candidate() {
                                    match if dry_run {
                                        Ok(unique_target_path(&transcode_dir, path, "jpg"))
                                    } else {
                                        transcode_to_jpeg(&img, path, &transcode_dir)
                                    } {
                                        Ok(p) => {
                                            let trans_path = p.to_string_lossy().to_string();
                                            (
                                                trans_path.clone(),
                                                if dry_run {
                                                    "would_transcode_to_jpeg".to_string()
                                                } else {
                                                    "transcoded_to_jpeg".to_string()
                                                },
                                                Some(trans_path),
                                            )
                                        }
                                        Err(e) => {
                                            error_count.fetch_add(1, Ordering::Relaxed);
                                            let output = ProcessResult::Error {
                                                path: original_path,
                                                message: format!("failed to transcode image: {}", e),
                                            };
                                            let processed = progress_count.fetch_add(1, Ordering::Relaxed) + 1;
                                            if processed % progress_step == 0 {
                                                pb_worker.set_position(processed);
                                            }
                                            return output;
                                        }
                                    }
                                } else {
                                    (original_path.clone(), "kept_jpeg".to_string(), None)
                                };

                                let hash = hasher.hash_image(&img).to_base64();
                                ProcessResult::Hashed {
                                    hash,
                                    original_path,
                                    stored_path,
                                    detected_format: detected.as_str().to_string(),
                                    original_extension,
                                    action_taken,
                                    quarantine_path: None,
                                    transcode_path,
                                    ingest_ts: OffsetDateTime::now_utc().unix_timestamp().to_string(),
                                }
                            }
                            Err(e) => match if dry_run {
                                let ext = path
                                    .extension()
                                    .and_then(|x| x.to_str())
                                    .unwrap_or("bin")
                                    .to_lowercase();
                                Ok(unique_target_path(&quarantine_dir, path, &ext))
                            } else {
                                quarantine_file(path, &quarantine_dir)
                            } {
                                Ok(quarantined) => {
                                    error_count.fetch_add(1, Ordering::Relaxed);
                                    ProcessResult::Quarantined {
                                        path: original_path,
                                        detected_format: detected.as_str().to_string(),
                                        message: if dry_run {
                                            format!("would quarantine decode failure (truncated/corrupt): {}", e)
                                        } else {
                                            format!("decode failed (truncated/corrupt): {}", e)
                                        },
                                        quarantine_path: quarantined.to_string_lossy().to_string(),
                                    }
                                }
                                Err(move_err) => {
                                    error_count.fetch_add(1, Ordering::Relaxed);
                                    ProcessResult::Error {
                                        path: original_path,
                                        message: format!(
                                            "decode failed (truncated/corrupt): {}; quarantine move failed: {}",
                                            e, move_err
                                        ),
                                    }
                                }
                            },
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

        if dry_run {
            for result in results {
                match result {
                    ProcessResult::Hashed { action_taken, .. } => {
                        if action_taken == "would_transcode_to_jpeg" {
                            transcoded_total += 1;
                        }
                        total_inserted += 1;
                    }
                    ProcessResult::Quarantined { path, detected_format, message, quarantine_path } => {
                        quarantined_total += 1;
                        if console_errors_printed < MAX_CONSOLE_ERRORS {
                            eprintln!(
                                "Dry run: {} (detected={}, reason={}, target={})",
                                path, detected_format, message, quarantine_path
                            );
                            console_errors_printed += 1;
                        } else {
                            console_errors_suppressed += 1;
                        }
                    }
                    ProcessResult::Error { path, message } => {
                        if console_errors_printed < MAX_CONSOLE_ERRORS {
                            eprintln!("Dry run error: {} (reason: {})", path, message);
                            console_errors_printed += 1;
                        } else {
                            console_errors_suppressed += 1;
                        }
                    }
                }
            }
        } else {
            let tx = conn
                .as_mut()
                .expect("database connection should exist when not in dry-run")
                .transaction()?;
            {
                let mut insert_hash_stmt = tx.prepare_cached(
                    "INSERT OR IGNORE INTO hashes (
                        hash,
                        original_path,
                        stored_path,
                        detected_format,
                        original_extension,
                        action_taken,
                        quarantine_path,
                        transcode_path,
                        ingest_ts
                    )
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                )?;

                for result in results {
                    match result {
                        ProcessResult::Hashed {
                            hash,
                            original_path,
                            stored_path,
                            detected_format,
                            original_extension,
                            action_taken,
                            quarantine_path,
                            transcode_path,
                            ingest_ts,
                        } => {
                            if action_taken == "transcoded_to_jpeg" {
                                transcoded_total += 1;
                            }

                            let inserted = insert_hash_stmt.execute(params![
                                hash,
                                original_path,
                                stored_path,
                                detected_format,
                                original_extension,
                                action_taken,
                                quarantine_path,
                                transcode_path,
                                ingest_ts
                            ])?;
                            if inserted > 0 {
                                total_inserted += 1;
                            }
                        }
                        ProcessResult::Quarantined { path, detected_format, message, quarantine_path } => {
                            quarantined_total += 1;
                            if console_errors_printed < MAX_CONSOLE_ERRORS {
                                eprintln!(
                                    "Quarantined file: {} (detected={}, reason={}, quarantine={})",
                                    path, detected_format, message, quarantine_path
                                );
                                console_errors_printed += 1;
                            } else {
                                console_errors_suppressed += 1;
                            }

                            if let Some(writer) = error_log_writer.as_mut() {
                                let _ = writeln!(
                                    writer,
                                    "path={}\tdetected={}\taction=quarantined\treason={}\tquarantine_path={}",
                                    path, detected_format, message, quarantine_path
                                );
                            }
                        }
                        ProcessResult::Error { path, message } => {
                            if console_errors_printed < MAX_CONSOLE_ERRORS {
                                eprintln!("Could not open image: {} (reason: {})", path, message);
                                console_errors_printed += 1;
                            } else {
                                console_errors_suppressed += 1;
                            }

                            if let Some(writer) = error_log_writer.as_mut() {
                                let _ = writeln!(
                                    writer,
                                    "path={}\taction=error\treason={}",
                                    path, message
                                );
                            }
                        }
                    }
                }
            }
            tx.commit()?;
        }
    }

    pb.set_position(image_paths.len() as u64);
    pb.finish_with_message("Processing done");

    let elapsed = start.elapsed();
    let total = image_paths.len();
    let errors = error_count.load(Ordering::Relaxed);
    println!("Done. Elapsed time: {:.2?}", elapsed);
    println!("Total images processed: {}", total);
    println!("Total errors: {}", errors);
    println!("Total transcoded to JPG: {}", transcoded_total);
    println!("Total quarantined: {}", quarantined_total);
    if dry_run {
        println!("Dry run preview: hashable files that would be inserted: {}", total_inserted);
    } else {
        println!("Total unique hashes inserted this run: {}", total_inserted);
    }

    if let Some(mut writer) = error_log_writer {
        let _ = writer.flush();
    }

    if !dry_run && console_errors_suppressed > 0 {
        println!(
            "Suppressed {} additional image error logs on console. Full diagnostics: {}",
            console_errors_suppressed,
            error_log_path.display()
        );
    }

    Ok(())
}
