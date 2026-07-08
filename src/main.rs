use rusqlite::{params, Connection, Result};
use walkdir::WalkDir;
use img_hash::image::{self, DynamicImage, ImageOutputFormat};
use img_hash::{HashAlg, HasherConfig};
use crossbeam_channel::bounded;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use std::{env, fs, io};
use time::OffsetDateTime;

const DB_PATH: &str = "/media/PiTB/images.db";
const SEARCH_DIR: &str = "/media/PiTB/foofuck/MASTERPICS";
const ERROR_LOG_FILE: &str = "dupcheckerrs-errors.log";
const TRANSCODE_DIR_NAME: &str = "transcoded_jpg";
const QUARANTINE_DIR_NAME: &str = "quarantine";
const MAX_CONSOLE_ERRORS: u64 = 20;
const PATH_QUEUE_CAP: usize = 2048;
const RESULT_QUEUE_CAP: usize = 2048;

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

fn detect_format_from_path(path: &Path) -> DetectedFormat {
    let mut file = match fs::File::open(path) {
        Ok(file) => file,
        Err(_) => return DetectedFormat::Unknown,
    };

    let mut header = [0u8; 64];
    let read = match file.read(&mut header) {
        Ok(n) => n,
        Err(_) => return DetectedFormat::Unknown,
    };

    if read == 0 {
        DetectedFormat::Unknown
    } else {
        detect_format(&header[..read])
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
    img.write_to(&mut writer, ImageOutputFormat::Jpeg(95))
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

fn worker_count() -> usize {
    env::var("DUPCHECKERRS_WORKERS")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(2)
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

    let workers = worker_count();
    println!(
        "Using {} worker threads (override with DUPCHECKERRS_WORKERS)",
        workers
    );

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

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} [{elapsed_precise}] {msg}")
            .unwrap(),
    );
    pb.enable_steady_tick(std::time::Duration::from_millis(120));

    let mut total_inserted = 0u64;
    let mut processed_total = 0u64;
    let mut errors = 0u64;
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

    let run_ingest_ts = OffsetDateTime::now_utc().unix_timestamp().to_string();
    let discovered_count = Arc::new(AtomicU64::new(0));

    let (path_tx, path_rx) = bounded::<PathBuf>(PATH_QUEUE_CAP);
    let (result_tx, result_rx) = bounded::<ProcessResult>(RESULT_QUEUE_CAP);

    let discover_counter = Arc::clone(&discovered_count);
    let scanner = thread::spawn(move || {
        for entry in WalkDir::new(SEARCH_DIR).into_iter().filter_map(|e| e.ok()) {
            if !entry.path().is_file() {
                continue;
            }
            let ext = extension_of(entry.path());
            if ext == "jpg" || ext == "jpeg" {
                discover_counter.fetch_add(1, Ordering::Relaxed);
                if path_tx.send(entry.path().to_owned()).is_err() {
                    break;
                }
            }
        }
    });

    let mut workers_join = Vec::with_capacity(workers);
    for _ in 0..workers {
        let worker_rx = path_rx.clone();
        let worker_tx = result_tx.clone();
        let worker_transcode_dir = transcode_dir.clone();
        let worker_quarantine_dir = quarantine_dir.clone();
        let worker_ingest_ts = run_ingest_ts.clone();

        workers_join.push(thread::spawn(move || {
            let hasher = HasherConfig::new().hash_alg(HashAlg::Mean).to_hasher();
            for path in worker_rx.iter() {
                let original_path = path.to_string_lossy().to_string();
                let original_extension = extension_of(&path);
                let detected = detect_format_from_path(&path);

                let output = match detected {
                    DetectedFormat::Unknown => match if dry_run {
                        let ext = path
                            .extension()
                            .and_then(|e| e.to_str())
                            .unwrap_or("bin")
                            .to_lowercase();
                        Ok(unique_target_path(&worker_quarantine_dir, &path, &ext))
                    } else {
                        quarantine_file(&path, &worker_quarantine_dir)
                    } {
                        Ok(quarantined) => ProcessResult::Quarantined {
                            path: original_path,
                            detected_format: detected.as_str().to_string(),
                            message: if dry_run {
                                "would quarantine unknown file signature".to_string()
                            } else {
                                "unknown file signature".to_string()
                            },
                            quarantine_path: quarantined.to_string_lossy().to_string(),
                        },
                        Err(e) => ProcessResult::Error {
                            path: original_path,
                            message: format!(
                                "unknown file signature and quarantine move failed: {}",
                                e
                            ),
                        },
                    },
                    _ => match image::open(&path) {
                        Ok(img) => {
                            let (stored_path, action_taken, transcode_path) =
                                if matches!(detected, DetectedFormat::Jpeg) {
                                    (original_path.clone(), "kept_jpeg".to_string(), None)
                                } else if detected.is_transcode_candidate() {
                                    match if dry_run {
                                        Ok(unique_target_path(&worker_transcode_dir, &path, "jpg"))
                                    } else {
                                        transcode_to_jpeg(&img, &path, &worker_transcode_dir)
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
                                            if worker_tx
                                                .send(ProcessResult::Error {
                                                    path: original_path,
                                                    message: format!(
                                                        "failed to transcode image: {}",
                                                        e
                                                    ),
                                                })
                                                .is_err()
                                            {
                                                break;
                                            }
                                            continue;
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
                                ingest_ts: worker_ingest_ts.clone(),
                            }
                        }
                        Err(e) => match if dry_run {
                            let ext = path
                                .extension()
                                .and_then(|x| x.to_str())
                                .unwrap_or("bin")
                                .to_lowercase();
                            Ok(unique_target_path(&worker_quarantine_dir, &path, &ext))
                        } else {
                            quarantine_file(&path, &worker_quarantine_dir)
                        } {
                            Ok(quarantined) => ProcessResult::Quarantined {
                                path: original_path,
                                detected_format: detected.as_str().to_string(),
                                message: if dry_run {
                                    format!("would quarantine decode failure (truncated/corrupt): {}", e)
                                } else {
                                    format!("decode failed (truncated/corrupt): {}", e)
                                },
                                quarantine_path: quarantined.to_string_lossy().to_string(),
                            },
                            Err(move_err) => ProcessResult::Error {
                                path: original_path,
                                message: format!(
                                    "decode failed (truncated/corrupt): {}; quarantine move failed: {}",
                                    e, move_err
                                ),
                            },
                        },
                    },
                };

                if worker_tx.send(output).is_err() {
                    break;
                }
            }
        }));
    }

    drop(path_rx);
    drop(result_tx);

    if dry_run {
        for result in result_rx.iter() {
            processed_total += 1;
            match result {
                ProcessResult::Hashed { action_taken, .. } => {
                    if action_taken == "would_transcode_to_jpeg" {
                        transcoded_total += 1;
                    }
                    total_inserted += 1;
                }
                ProcessResult::Quarantined {
                    path,
                    detected_format,
                    message,
                    quarantine_path,
                } => {
                    errors += 1;
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
                    errors += 1;
                    if console_errors_printed < MAX_CONSOLE_ERRORS {
                        eprintln!("Dry run error: {} (reason: {})", path, message);
                        console_errors_printed += 1;
                    } else {
                        console_errors_suppressed += 1;
                    }
                }
            }

            if processed_total % 128 == 0 {
                let discovered = discovered_count.load(Ordering::Relaxed);
                pb.set_message(format!("discovered={} processed={}", discovered, processed_total));
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

            for result in result_rx.iter() {
                processed_total += 1;
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
                    ProcessResult::Quarantined {
                        path,
                        detected_format,
                        message,
                        quarantine_path,
                    } => {
                        errors += 1;
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
                        errors += 1;
                        if console_errors_printed < MAX_CONSOLE_ERRORS {
                            eprintln!("Could not open image: {} (reason: {})", path, message);
                            console_errors_printed += 1;
                        } else {
                            console_errors_suppressed += 1;
                        }

                        if let Some(writer) = error_log_writer.as_mut() {
                            let _ = writeln!(writer, "path={}\taction=error\treason={}", path, message);
                        }
                    }
                }

                if processed_total % 128 == 0 {
                    let discovered = discovered_count.load(Ordering::Relaxed);
                    pb.set_message(format!("discovered={} processed={}", discovered, processed_total));
                }
            }
        }
        tx.commit()?;
    }

    if scanner.join().is_err() {
        eprintln!("Warning: scanner thread terminated unexpectedly");
    }
    for worker in workers_join {
        if worker.join().is_err() {
            eprintln!("Warning: worker thread terminated unexpectedly");
        }
    }

    let total_discovered = discovered_count.load(Ordering::Relaxed);
    pb.set_message(format!("discovered={} processed={}", total_discovered, processed_total));
    pb.finish_with_message("Processing done");

    let elapsed = start.elapsed();
    println!("Done. Elapsed time: {:.2?}", elapsed);
    println!("Total images discovered: {}", total_discovered);
    println!("Total images processed: {}", processed_total);
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
