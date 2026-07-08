use rusqlite::{params, Connection, Result};
use walkdir::WalkDir;
use img_hash::image::{self, DynamicImage, ImageOutputFormat};
use img_hash::{HashAlg, HasherConfig};
use crossbeam_channel::{bounded, RecvTimeoutError};
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use std::{env, fs, io};
use time::OffsetDateTime;

const ENV_DRY_RUN: &str = "DUPCHECKERRS_DRY_RUN";
const ENV_WORKERS: &str = "DUPCHECKERRS_WORKERS";
const ENV_HEARTBEAT_SECS: &str = "DUPCHECKERRS_HEARTBEAT_SECS";
const ENV_STALL_WARN_SECS: &str = "DUPCHECKERRS_STALL_WARN_SECS";
const ENV_NO_PROGRESS: &str = "DUPCHECKERRS_NO_PROGRESS";
const ENV_DB_PATH: &str = "DUPCHECKERRS_DB_PATH";
const ENV_SEARCH_DIR: &str = "DUPCHECKERRS_SEARCH_DIR";
const ENV_ERROR_LOG_FILE: &str = "DUPCHECKERRS_ERROR_LOG_FILE";
const ENV_TRANSCODE_DIR_NAME: &str = "DUPCHECKERRS_TRANSCODE_DIR_NAME";
const ENV_QUARANTINE_DIR_NAME: &str = "DUPCHECKERRS_QUARANTINE_DIR_NAME";
const ENV_MAX_CONSOLE_ERRORS: &str = "DUPCHECKERRS_MAX_CONSOLE_ERRORS";
const ENV_PATH_QUEUE_CAP: &str = "DUPCHECKERRS_PATH_QUEUE_CAP";
const ENV_RESULT_QUEUE_CAP: &str = "DUPCHECKERRS_RESULT_QUEUE_CAP";
const ENV_JPEG_QUALITY: &str = "DUPCHECKERRS_JPEG_QUALITY";

const DEFAULT_DB_PATH: &str = "/media/PiTB/images.db";
const DEFAULT_SEARCH_DIR: &str = "/media/PiTB/foofuck/Camera1";
const DEFAULT_ERROR_LOG_FILE: &str = "dupcheckerrs-errors.log";
const DEFAULT_TRANSCODE_DIR_NAME: &str = "transcoded_jpg";
const DEFAULT_QUARANTINE_DIR_NAME: &str = "quarantine";
const DEFAULT_MAX_CONSOLE_ERRORS: u64 = 20;
const DEFAULT_PATH_QUEUE_CAP: usize = 2048;
const DEFAULT_RESULT_QUEUE_CAP: usize = 2048;
const DEFAULT_HEARTBEAT_SECS: u64 = 15;
const DEFAULT_STALL_WARN_SECS: u64 = 120;
const DEFAULT_WORKERS: usize = 3;
const DEFAULT_JPEG_QUALITY: u8 = 95;

struct RuntimeConfig {
    dry_run: bool,
    workers: usize,
    heartbeat_secs: u64,
    stall_warn_secs: u64,
    progress_enabled: bool,
    db_path: String,
    search_dir: String,
    error_log_file: String,
    transcode_dir_name: String,
    quarantine_dir_name: String,
    max_console_errors: u64,
    path_queue_cap: usize,
    result_queue_cap: usize,
    jpeg_quality: u8,
}

impl RuntimeConfig {
    fn from_env() -> Self {
        let jpeg_quality = env_u8(ENV_JPEG_QUALITY, DEFAULT_JPEG_QUALITY).clamp(1, 100);
        RuntimeConfig {
            dry_run: env_flag(ENV_DRY_RUN),
            workers: env_usize(ENV_WORKERS, DEFAULT_WORKERS),
            heartbeat_secs: env_u64(ENV_HEARTBEAT_SECS, DEFAULT_HEARTBEAT_SECS),
            stall_warn_secs: env_u64(ENV_STALL_WARN_SECS, DEFAULT_STALL_WARN_SECS),
            progress_enabled: !env_flag(ENV_NO_PROGRESS),
            db_path: env_string(ENV_DB_PATH, DEFAULT_DB_PATH),
            search_dir: env_string(ENV_SEARCH_DIR, DEFAULT_SEARCH_DIR),
            error_log_file: env_string(ENV_ERROR_LOG_FILE, DEFAULT_ERROR_LOG_FILE),
            transcode_dir_name: env_string(ENV_TRANSCODE_DIR_NAME, DEFAULT_TRANSCODE_DIR_NAME),
            quarantine_dir_name: env_string(ENV_QUARANTINE_DIR_NAME, DEFAULT_QUARANTINE_DIR_NAME),
            max_console_errors: env_u64(ENV_MAX_CONSOLE_ERRORS, DEFAULT_MAX_CONSOLE_ERRORS),
            path_queue_cap: env_usize(ENV_PATH_QUEUE_CAP, DEFAULT_PATH_QUEUE_CAP),
            result_queue_cap: env_usize(ENV_RESULT_QUEUE_CAP, DEFAULT_RESULT_QUEUE_CAP),
            jpeg_quality,
        }
    }
}

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

fn transcode_to_jpeg(
    img: &DynamicImage,
    source: &Path,
    transcode_dir: &Path,
    jpeg_quality: u8,
) -> io::Result<PathBuf> {
    let out_path = unique_target_path(transcode_dir, source, "jpg");
    let file = fs::File::create(&out_path)?;
    let mut writer = BufWriter::new(file);
    img.write_to(&mut writer, ImageOutputFormat::Jpeg(jpeg_quality))
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

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn env_u8(name: &str, default: u8) -> u8 {
    env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<u8>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn env_string(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

fn format_duration(total_secs: u64) -> String {
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;
    format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
}

fn main() -> Result<()> {
    let start = Instant::now();
    let config = RuntimeConfig::from_env();
    let dry_run = config.dry_run;
    let cwd = env::current_dir().expect("failed to read current working directory");
    let transcode_dir = cwd.join(&config.transcode_dir_name);
    let quarantine_dir = cwd.join(&config.quarantine_dir_name);
    let error_log_path = cwd.join(&config.error_log_file);

    if dry_run {
        println!(
            "Dry run enabled ({}): previewing actions without DB writes, transcodes, or file moves",
            ENV_DRY_RUN
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

    let workers = config.workers;
    let heartbeat_secs = config.heartbeat_secs;
    let stall_warn_secs = config.stall_warn_secs;
    let progress_enabled = config.progress_enabled;
    println!(
        "Using {} worker threads (override with {})",
        workers, ENV_WORKERS
    );
    eprintln!(
        "Runtime config: search_dir={} db_path={} heartbeat={}s stall_warn={}s progress_ui={} jpeg_quality={} queue_caps=({}, {}) max_console_errors={}",
        config.search_dir,
        config.db_path,
        heartbeat_secs,
        stall_warn_secs,
        progress_enabled,
        config.jpeg_quality,
        config.path_queue_cap,
        config.result_queue_cap,
        config.max_console_errors
    );

    // Open and initialize the database only when not in dry-run mode.
    let mut conn = if dry_run {
        None
    } else {
        let conn = Connection::open(&config.db_path)?;
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
    if progress_enabled {
        pb.enable_steady_tick(Duration::from_millis(600));
    } else {
        pb.disable_steady_tick();
        pb.set_draw_target(indicatif::ProgressDrawTarget::hidden());
    }

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
    let scanner_done = Arc::new(AtomicBool::new(false));

    let (path_tx, path_rx) = bounded::<PathBuf>(config.path_queue_cap);
    let (result_tx, result_rx) = bounded::<ProcessResult>(config.result_queue_cap);

    let discover_counter = Arc::clone(&discovered_count);
    let scanner_done_flag = Arc::clone(&scanner_done);
    let search_dir = config.search_dir.clone();
    let scanner = thread::spawn(move || {
        for entry in WalkDir::new(search_dir).into_iter().filter_map(|e| e.ok()) {
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
        scanner_done_flag.store(true, Ordering::Relaxed);
    });

    let mut workers_join = Vec::with_capacity(workers);
    for _ in 0..workers {
        let worker_rx = path_rx.clone();
        let worker_tx = result_tx.clone();
        let worker_transcode_dir = transcode_dir.clone();
        let worker_quarantine_dir = quarantine_dir.clone();
        let worker_ingest_ts = run_ingest_ts.clone();
        let worker_jpeg_quality = config.jpeg_quality;

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
                                        transcode_to_jpeg(
                                            &img,
                                            &path,
                                            &worker_transcode_dir,
                                            worker_jpeg_quality,
                                        )
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

    let heartbeat_interval = Duration::from_secs(heartbeat_secs);
    let stall_warn_interval = Duration::from_secs(stall_warn_secs);
    let mut last_heartbeat_at = Instant::now();
    let mut last_heartbeat_processed = 0u64;
    let mut last_progress_at = Instant::now();
    let mut last_stall_warn_at = Instant::now();

    drop(result_tx);

    if dry_run {
        loop {
            match result_rx.recv_timeout(Duration::from_millis(500)) {
                Ok(result) => {
                    processed_total += 1;
                    last_progress_at = Instant::now();
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
                            if console_errors_printed < config.max_console_errors {
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
                            if console_errors_printed < config.max_console_errors {
                                eprintln!("Dry run error: {} (reason: {})", path, message);
                                console_errors_printed += 1;
                            } else {
                                console_errors_suppressed += 1;
                            }
                        }
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }

            let now = Instant::now();
            if now.duration_since(last_heartbeat_at) >= heartbeat_interval {
                let discovered = discovered_count.load(Ordering::Relaxed);
                let scan_done = scanner_done.load(Ordering::Relaxed);
                let elapsed_secs = start.elapsed().as_secs_f64();
                let avg_rate = if elapsed_secs > 0.0 {
                    processed_total as f64 / elapsed_secs
                } else {
                    0.0
                };
                let hb_delta_secs = now.duration_since(last_heartbeat_at).as_secs_f64();
                let hb_delta_processed = processed_total.saturating_sub(last_heartbeat_processed);
                let instant_rate = if hb_delta_secs > 0.0 {
                    hb_delta_processed as f64 / hb_delta_secs
                } else {
                    0.0
                };
                let pct = if scan_done && discovered > 0 {
                    (processed_total as f64 / discovered as f64) * 100.0
                } else {
                    0.0
                };

                let eta = if scan_done && discovered > processed_total && instant_rate > 0.01 {
                    let remaining = discovered - processed_total;
                    let eta_secs = (remaining as f64 / instant_rate) as u64;
                    format_duration(eta_secs)
                } else {
                    "n/a".to_string()
                };

                eprintln!(
                    "HEARTBEAT elapsed={} discovered={} processed={} errors={} transcodes={} quarantined={} avg_rate={:.2}/s inst_rate={:.2}/s scan_done={} pct={:.1}% eta={} q_path={} q_result={}",
                    format_duration(start.elapsed().as_secs()),
                    discovered,
                    processed_total,
                    errors,
                    transcoded_total,
                    quarantined_total,
                    avg_rate,
                    instant_rate,
                    scan_done,
                    pct,
                    eta,
                    path_rx.len(),
                    result_rx.len()
                );

                pb.set_message(format!(
                    "discovered={} processed={} err={} rate={:.2}/s",
                    discovered, processed_total, errors, avg_rate
                ));
                last_heartbeat_at = now;
                last_heartbeat_processed = processed_total;
            }

            if now.duration_since(last_progress_at) >= stall_warn_interval
                && now.duration_since(last_stall_warn_at) >= heartbeat_interval
            {
                eprintln!(
                    "WARN stall detected: no progress for {}s (discovered={} processed={} q_path={} q_result={} scan_done={})",
                    now.duration_since(last_progress_at).as_secs(),
                    discovered_count.load(Ordering::Relaxed),
                    processed_total,
                    path_rx.len(),
                    result_rx.len(),
                    scanner_done.load(Ordering::Relaxed)
                );
                last_stall_warn_at = now;
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

            loop {
                match result_rx.recv_timeout(Duration::from_millis(500)) {
                    Ok(result) => {
                        processed_total += 1;
                        last_progress_at = Instant::now();
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
                                if console_errors_printed < config.max_console_errors {
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
                                if console_errors_printed < config.max_console_errors {
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
                    }
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => break,
                }

                let now = Instant::now();
                if now.duration_since(last_heartbeat_at) >= heartbeat_interval {
                    let discovered = discovered_count.load(Ordering::Relaxed);
                    let scan_done = scanner_done.load(Ordering::Relaxed);
                    let elapsed_secs = start.elapsed().as_secs_f64();
                    let avg_rate = if elapsed_secs > 0.0 {
                        processed_total as f64 / elapsed_secs
                    } else {
                        0.0
                    };
                    let hb_delta_secs = now.duration_since(last_heartbeat_at).as_secs_f64();
                    let hb_delta_processed = processed_total.saturating_sub(last_heartbeat_processed);
                    let instant_rate = if hb_delta_secs > 0.0 {
                        hb_delta_processed as f64 / hb_delta_secs
                    } else {
                        0.0
                    };
                    let pct = if scan_done && discovered > 0 {
                        (processed_total as f64 / discovered as f64) * 100.0
                    } else {
                        0.0
                    };

                    let eta = if scan_done && discovered > processed_total && instant_rate > 0.01 {
                        let remaining = discovered - processed_total;
                        let eta_secs = (remaining as f64 / instant_rate) as u64;
                        format_duration(eta_secs)
                    } else {
                        "n/a".to_string()
                    };

                    eprintln!(
                        "HEARTBEAT elapsed={} discovered={} processed={} errors={} transcodes={} quarantined={} avg_rate={:.2}/s inst_rate={:.2}/s scan_done={} pct={:.1}% eta={} q_path={} q_result={}",
                        format_duration(start.elapsed().as_secs()),
                        discovered,
                        processed_total,
                        errors,
                        transcoded_total,
                        quarantined_total,
                        avg_rate,
                        instant_rate,
                        scan_done,
                        pct,
                        eta,
                        path_rx.len(),
                        result_rx.len()
                    );

                    pb.set_message(format!(
                        "discovered={} processed={} err={} rate={:.2}/s",
                        discovered, processed_total, errors, avg_rate
                    ));
                    last_heartbeat_at = now;
                    last_heartbeat_processed = processed_total;
                }

                if now.duration_since(last_progress_at) >= stall_warn_interval
                    && now.duration_since(last_stall_warn_at) >= heartbeat_interval
                {
                    eprintln!(
                        "WARN stall detected: no progress for {}s (discovered={} processed={} q_path={} q_result={} scan_done={})",
                        now.duration_since(last_progress_at).as_secs(),
                        discovered_count.load(Ordering::Relaxed),
                        processed_total,
                        path_rx.len(),
                        result_rx.len(),
                        scanner_done.load(Ordering::Relaxed)
                    );
                    last_stall_warn_at = now;
                }
            }
        }
        tx.commit()?;
    }

    drop(path_rx);

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
