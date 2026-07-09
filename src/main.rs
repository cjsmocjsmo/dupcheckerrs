use rusqlite::{params, Connection, Result};
use walkdir::WalkDir;
use img_hash::image::{self, DynamicImage, ImageOutputFormat, GrayImage};
use img_hash::image::imageops::FilterType;
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
use ffmpeg_light::generate_thumbnail;
use ffmpeg_light::probe;
use ffmpeg_light::thumbnail::{ThumbnailFormat, ThumbnailOptions};
use ffmpeg_light::types::Time;
use time::OffsetDateTime;
use turbojpeg;

// Set all runtime behavior here, then build on rpi4 and copy only the binary to rpi3b+.
const DUPCHECKER_DRY_RUN: bool = false;
const DUPCHECKER_WORKERS: usize = 3;
const DUPCHECKER_HEARTBEAT_SECS: u64 = 15;
const DUPCHECKER_STALL_WARN_SECS: u64 = 120;
const DUPCHECKER_PROGRESS_ENABLED: bool = true;
const DUPCHECKER_DB_PATH: &str = "/media/PiTB/images.db";
const DUPCHECKER_SEARCH_DIR: &str = "/media/PiTB/foofuck/CameraT";
const DUPCHECKER_ERROR_LOG_FILE: &str = "dupcheckerrs-errors.log";
const DUPCHECKER_TRANSCODE_DIR_NAME: &str = "transcoded_jpg";
const DUPCHECKER_QUARANTINE_DIR_NAME: &str = "quarantine";
const DUPCHECKER_MAX_CONSOLE_ERRORS: u64 = 20;
const DUPCHECKER_PATH_QUEUE_CAP: usize = 2048;
const DUPCHECKER_RESULT_QUEUE_CAP: usize = 2048;
const DUPCHECKER_JPEG_QUALITY: u8 = 95;
const DUPCHECKER_HASH_DOWNSCALE_SIZE: u32 = 124;
const DUPCHECKER_MOVIE_WORKERS: usize = 1;
const DUPCHECKER_MOVIE_FRAME_SAMPLES: usize = 5;
const DUPCHECKER_MOVIE_PATH_QUEUE_CAP: usize = 512;
const DUPCHECKER_MOVIE_RESULT_QUEUE_CAP: usize = 512;

// Optional compatibility mode: when true, environment variables can override the constants above.
const ENABLE_ENV_OVERRIDES: bool = false;
const ENV_DRY_RUN: &str = "DUPCHECKER_DRY_RUN";
const ENV_WORKERS: &str = "DUPCHECKER_WORKERS";
const ENV_HEARTBEAT_SECS: &str = "DUPCHECKER_HEARTBEAT_SECS";
const ENV_STALL_WARN_SECS: &str = "DUPCHECKER_STALL_WARN_SECS";
const ENV_PROGRESS_ENABLED: &str = "DUPCHECKER_PROGRESS_ENABLED";
const ENV_DB_PATH: &str = "DUPCHECKER_DB_PATH";
const ENV_SEARCH_DIR: &str = "DUPCHECKER_SEARCH_DIR";
const ENV_ERROR_LOG_FILE: &str = "DUPCHECKER_ERROR_LOG_FILE";
const ENV_TRANSCODE_DIR_NAME: &str = "DUPCHECKER_TRANSCODE_DIR_NAME";
const ENV_QUARANTINE_DIR_NAME: &str = "DUPCHECKER_QUARANTINE_DIR_NAME";
const ENV_MAX_CONSOLE_ERRORS: &str = "DUPCHECKER_MAX_CONSOLE_ERRORS";
const ENV_PATH_QUEUE_CAP: &str = "DUPCHECKER_PATH_QUEUE_CAP";
const ENV_RESULT_QUEUE_CAP: &str = "DUPCHECKER_RESULT_QUEUE_CAP";
const ENV_JPEG_QUALITY: &str = "DUPCHECKER_JPEG_QUALITY";
const ENV_HASH_DOWNSCALE_SIZE: &str = "DUPCHECKER_HASH_DOWNSCALE_SIZE";

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
    hash_downscale_size: u32,
    movie_workers: usize,
    movie_frame_samples: usize,
    movie_path_queue_cap: usize,
    movie_result_queue_cap: usize,
}

impl RuntimeConfig {
    fn load() -> Self {
        let base = RuntimeConfig {
            dry_run: DUPCHECKER_DRY_RUN,
            workers: DUPCHECKER_WORKERS,
            heartbeat_secs: DUPCHECKER_HEARTBEAT_SECS,
            stall_warn_secs: DUPCHECKER_STALL_WARN_SECS,
            progress_enabled: DUPCHECKER_PROGRESS_ENABLED,
            db_path: DUPCHECKER_DB_PATH.to_string(),
            search_dir: DUPCHECKER_SEARCH_DIR.to_string(),
            error_log_file: DUPCHECKER_ERROR_LOG_FILE.to_string(),
            transcode_dir_name: DUPCHECKER_TRANSCODE_DIR_NAME.to_string(),
            quarantine_dir_name: DUPCHECKER_QUARANTINE_DIR_NAME.to_string(),
            max_console_errors: DUPCHECKER_MAX_CONSOLE_ERRORS,
            path_queue_cap: DUPCHECKER_PATH_QUEUE_CAP,
            result_queue_cap: DUPCHECKER_RESULT_QUEUE_CAP,
            jpeg_quality: DUPCHECKER_JPEG_QUALITY.clamp(1, 100),
            hash_downscale_size: DUPCHECKER_HASH_DOWNSCALE_SIZE.clamp(8, 512),
            movie_workers: DUPCHECKER_MOVIE_WORKERS.max(1),
            movie_frame_samples: DUPCHECKER_MOVIE_FRAME_SAMPLES.max(1),
            movie_path_queue_cap: DUPCHECKER_MOVIE_PATH_QUEUE_CAP.max(1),
            movie_result_queue_cap: DUPCHECKER_MOVIE_RESULT_QUEUE_CAP.max(1),
        };

        if ENABLE_ENV_OVERRIDES {
            RuntimeConfig {
                dry_run: env_flag(ENV_DRY_RUN) || base.dry_run,
                workers: env_usize(ENV_WORKERS, base.workers),
                heartbeat_secs: env_u64(ENV_HEARTBEAT_SECS, base.heartbeat_secs),
                stall_warn_secs: env_u64(ENV_STALL_WARN_SECS, base.stall_warn_secs),
                progress_enabled: env_flag(ENV_PROGRESS_ENABLED) || base.progress_enabled,
                db_path: env_string(ENV_DB_PATH, &base.db_path),
                search_dir: env_string(ENV_SEARCH_DIR, &base.search_dir),
                error_log_file: env_string(ENV_ERROR_LOG_FILE, &base.error_log_file),
                transcode_dir_name: env_string(ENV_TRANSCODE_DIR_NAME, &base.transcode_dir_name),
                quarantine_dir_name: env_string(ENV_QUARANTINE_DIR_NAME, &base.quarantine_dir_name),
                max_console_errors: env_u64(ENV_MAX_CONSOLE_ERRORS, base.max_console_errors),
                path_queue_cap: env_usize(ENV_PATH_QUEUE_CAP, base.path_queue_cap),
                result_queue_cap: env_usize(ENV_RESULT_QUEUE_CAP, base.result_queue_cap),
                jpeg_quality: env_u8(ENV_JPEG_QUALITY, base.jpeg_quality).clamp(1, 100),
                hash_downscale_size: env_u32(ENV_HASH_DOWNSCALE_SIZE, base.hash_downscale_size)
                    .clamp(8, 512),
                movie_workers: base.movie_workers,
                movie_frame_samples: base.movie_frame_samples,
                movie_path_queue_cap: base.movie_path_queue_cap,
                movie_result_queue_cap: base.movie_result_queue_cap,
            }
        } else {
            base
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

#[derive(Clone, Copy)]
enum DetectedVideoFormat {
    Mp4,
    Mov,
    M4v,
    Mkv,
    Webm,
    Avi,
    Mpg,
    Mpeg,
    Unknown,
}

impl DetectedVideoFormat {
    fn as_str(self) -> &'static str {
        match self {
            DetectedVideoFormat::Mp4 => "mp4",
            DetectedVideoFormat::Mov => "mov",
            DetectedVideoFormat::M4v => "m4v",
            DetectedVideoFormat::Mkv => "mkv",
            DetectedVideoFormat::Webm => "webm",
            DetectedVideoFormat::Avi => "avi",
            DetectedVideoFormat::Mpg => "mpg",
            DetectedVideoFormat::Mpeg => "mpeg",
            DetectedVideoFormat::Unknown => "unknown",
        }
    }
}

enum MovieProcessResult {
    Hashed {
        hash: String,
        original_path: String,
        detected_format: String,
        extension: String,
        ingest_ts: String,
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

fn is_image_extension(ext: &str) -> bool {
    matches!(ext, "jpg" | "jpeg" | "png" | "gif" | "webp")
}

fn is_movie_extension(ext: &str) -> bool {
    matches!(ext, "mp4" | "mov" | "m4v" | "mkv" | "webm" | "avi" | "mpg" | "mpeg")
}

fn detect_video_format_from_path(path: &Path) -> DetectedVideoFormat {
    let ext = extension_of(path);

    let mut file = match fs::File::open(path) {
        Ok(file) => file,
        Err(_) => return format_from_video_extension(&ext),
    };

    let mut header = [0u8; 64];
    let read = match file.read(&mut header) {
        Ok(n) => n,
        Err(_) => 0,
    };

    if read >= 12 && &header[4..8] == b"ftyp" {
        let brand = &header[8..12];
        if brand == b"qt  " {
            return DetectedVideoFormat::Mov;
        }
        if brand == b"M4V " {
            return DetectedVideoFormat::M4v;
        }
        return DetectedVideoFormat::Mp4;
    }

    if read >= 12 && &header[0..4] == b"RIFF" && &header[8..12] == b"AVI " {
        return DetectedVideoFormat::Avi;
    }

    if read >= 4 && header[0..4] == [0x1A, 0x45, 0xDF, 0xA3] {
        return match ext.as_str() {
            "webm" => DetectedVideoFormat::Webm,
            "mkv" => DetectedVideoFormat::Mkv,
            _ => DetectedVideoFormat::Unknown,
        };
    }

    if read >= 4 && header[0..4] == [0x00, 0x00, 0x01, 0xBA] {
        return match ext.as_str() {
            "mpeg" => DetectedVideoFormat::Mpeg,
            _ => DetectedVideoFormat::Mpg,
        };
    }

    format_from_video_extension(&ext)
}

fn format_from_video_extension(ext: &str) -> DetectedVideoFormat {
    match ext {
        "mp4" => DetectedVideoFormat::Mp4,
        "mov" => DetectedVideoFormat::Mov,
        "m4v" => DetectedVideoFormat::M4v,
        "mkv" => DetectedVideoFormat::Mkv,
        "webm" => DetectedVideoFormat::Webm,
        "avi" => DetectedVideoFormat::Avi,
        "mpg" => DetectedVideoFormat::Mpg,
        "mpeg" => DetectedVideoFormat::Mpeg,
        _ => DetectedVideoFormat::Unknown,
    }
}

fn hash_video_perceptual(
    path: &Path,
    hash_downscale_size: u32,
    frame_samples: usize,
) -> std::result::Result<String, String> {
    let probe_result = probe(path).map_err(|e| format!("ffprobe failed: {}", e))?;
    let duration = probe_result
        .duration()
        .ok_or_else(|| "video duration unavailable from ffprobe".to_string())?;

    let duration_secs = duration.as_secs_f64();
    if duration_secs <= 0.0 {
        return Err("video duration is zero".to_string());
    }

    let target_size = hash_downscale_size.max(8);
    let sample_target = frame_samples.max(1);
    let hasher = HasherConfig::new().hash_alg(HashAlg::Mean).to_hasher();

    let temp_root = env::temp_dir().join("dupcheckerrs-movie-frames");
    fs::create_dir_all(&temp_root)
        .map_err(|e| format!("failed to create movie temp dir {}: {}", temp_root.display(), e))?;

    let source_id = path_fingerprint(path);
    let unique_run = OffsetDateTime::now_utc().unix_timestamp_nanos();
    let mut frame_hashes: Vec<String> = Vec::with_capacity(sample_target);

    for idx in 0..sample_target {
        let sample_ratio = (idx + 1) as f64 / (sample_target + 1) as f64;
        let sample_secs = (duration_secs * sample_ratio).max(0.001);
        let frame_time = Time::from_seconds_f64(sample_secs);

        let frame_path = temp_root.join(format!(
            "movie_{}_{}_{}_{}.jpg",
            source_id,
            unique_run,
            std::process::id(),
            idx
        ));

        let thumb_opts = ThumbnailOptions::new(frame_time)
            .size(target_size, target_size)
            .format(ThumbnailFormat::Jpeg);

        if let Err(e) = generate_thumbnail(path, &frame_path, &thumb_opts) {
            let _ = fs::remove_file(&frame_path);
            return Err(format!(
                "ffmpeg thumbnail generation failed at {:.3}s: {}",
                sample_secs, e
            ));
        }

        let frame_hash = match image::open(&frame_path) {
            Ok(img) => {
                let gray = image::imageops::resize(
                    &img.to_luma8(),
                    target_size,
                    target_size,
                    FilterType::Triangle,
                );
                hasher
                    .hash_image(&DynamicImage::ImageLuma8(gray))
                    .to_base64()
            }
            Err(e) => {
                let _ = fs::remove_file(&frame_path);
                return Err(format!(
                    "failed to open generated movie frame {}: {}",
                    frame_path.display(),
                    e
                ));
            }
        };

        let _ = fs::remove_file(&frame_path);
        frame_hashes.push(frame_hash);
    }

    if frame_hashes.is_empty() {
        return Err("no decodable video frames".to_string());
    }

    let mut aggregate = DefaultHasher::new();
    for frame_hash in frame_hashes {
        frame_hash.hash(&mut aggregate);
    }
    Ok(format!("{:016x}", aggregate.finish()))
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

    fn env_u32(name: &str, default: u32) -> u32 {
        env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<u32>().ok())
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
    let config = RuntimeConfig::load();
    let dry_run = config.dry_run;
    let cwd = env::current_dir().expect("failed to read current working directory");
    let transcode_dir = cwd.join(&config.transcode_dir_name);
    let quarantine_dir = cwd.join(&config.quarantine_dir_name);
    let error_log_path = cwd.join(&config.error_log_file);

    if dry_run {
        println!(
            "Dry run enabled (set by DUPCHECKER_DRY_RUN in source): previewing actions without DB writes, transcodes, or file moves"
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
        "Using {} worker threads (set by DUPCHECKER_WORKERS in source)",
        workers
    );
    eprintln!(
        "Runtime config: search_dir={} db_path={} heartbeat={}s stall_warn={}s progress_ui={} jpeg_quality={} hash_downscale={} image_queue_caps=({}, {}) movie_workers={} movie_frame_samples={} movie_queue_caps=({}, {}) max_console_errors={} env_overrides={}",
        config.search_dir,
        config.db_path,
        heartbeat_secs,
        stall_warn_secs,
        progress_enabled,
        config.jpeg_quality,
        config.hash_downscale_size,
        config.path_queue_cap,
        config.result_queue_cap,
        config.movie_workers,
        config.movie_frame_samples,
        config.movie_path_queue_cap,
        config.movie_result_queue_cap,
        config.max_console_errors,
        ENABLE_ENV_OVERRIDES
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

        conn.execute(
            "CREATE TABLE IF NOT EXISTS movie_hashes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hash TEXT UNIQUE,
                original_path TEXT NOT NULL,
                detected_format TEXT NOT NULL,
                extension TEXT NOT NULL,
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
    let mut movie_processed_total = 0u64;
    let mut movie_inserted_total = 0u64;

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
            if is_image_extension(&ext) {
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
        let worker_hash_downscale_size = config.hash_downscale_size;

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
                    DetectedFormat::Jpeg => {
                        match fs::read(&path) {
                            Ok(jpeg_bytes) => match turbojpeg::decompress_to_yuv(&jpeg_bytes) {
                                Ok(yuv) => {
                                    let width = yuv.width;
                                    let height = yuv.height;
                                    let y_stride = yuv.y_width();
                                    let mut grayscale = vec![0u8; width * height];

                                    for row in 0..height {
                                        let src_start = row * y_stride;
                                        let src_end = src_start + width;
                                        let dst_start = row * width;
                                        grayscale[dst_start..dst_start + width]
                                            .copy_from_slice(&yuv.pixels[src_start..src_end]);
                                    }

                                    let gray_image = GrayImage::from_raw(
                                        width as u32,
                                        height as u32,
                                        grayscale,
                                    )
                                    .expect("grayscale buffer should match image dimensions");
                                    let downscaled = image::imageops::resize(
                                        &gray_image,
                                        worker_hash_downscale_size,
                                        worker_hash_downscale_size,
                                        FilterType::Triangle,
                                    );
                                    let hash = hasher.hash_image(&DynamicImage::ImageLuma8(downscaled)).to_base64();
                                    let stored_path = original_path.clone();

                                    ProcessResult::Hashed {
                                        hash,
                                        original_path,
                                        stored_path,
                                        detected_format: detected.as_str().to_string(),
                                        original_extension,
                                        action_taken: "kept_jpeg".to_string(),
                                        quarantine_path: None,
                                        transcode_path: None,
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
                                            format!("would quarantine jpeg decode failure: {}", e)
                                        } else {
                                            format!("jpeg decode failed: {}", e)
                                        },
                                        quarantine_path: quarantined.to_string_lossy().to_string(),
                                    },
                                    Err(move_err) => ProcessResult::Error {
                                        path: original_path,
                                        message: format!(
                                            "jpeg decode failed: {}; quarantine move failed: {}",
                                            e, move_err
                                        ),
                                    },
                                },
                            },
                            Err(e) => ProcessResult::Error {
                                path: original_path,
                                message: format!("failed to read jpeg bytes: {}", e),
                            },
                        }
                    }
                    _ => match image::open(&path) {
                        Ok(img) => {
                            let (stored_path, action_taken, transcode_path) =
                                if detected.is_transcode_candidate() {
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
                    "HEARTBEAT elapsed={} discovered={} processed={} errors={} transcodes={} quarantined={} avg_rate={:.2}/s inst_rate={:.2}/s scan_done={} pct={:.1}% eta={} hash_downscale={} q_path={} q_result={}",
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
                    config.hash_downscale_size,
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
                        "HEARTBEAT elapsed={} discovered={} processed={} errors={} transcodes={} quarantined={} avg_rate={:.2}/s inst_rate={:.2}/s scan_done={} pct={:.1}% eta={} hash_downscale={} q_path={} q_result={}",
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
                        config.hash_downscale_size,
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

    println!("Image phase complete. Starting movie phase...");
    let movie_ingest_ts = OffsetDateTime::now_utc().unix_timestamp().to_string();
    let movie_discovered = Arc::new(AtomicU64::new(0));

    let (movie_path_tx, movie_path_rx) = bounded::<PathBuf>(config.movie_path_queue_cap);
    let (movie_result_tx, movie_result_rx) = bounded::<MovieProcessResult>(config.movie_result_queue_cap);

    let movie_search_dir = config.search_dir.clone();
    let movie_discover_counter = Arc::clone(&movie_discovered);
    let movie_scanner = thread::spawn(move || {
        for entry in WalkDir::new(movie_search_dir).into_iter().filter_map(|e| e.ok()) {
            if !entry.path().is_file() {
                continue;
            }
            let ext = extension_of(entry.path());
            if is_movie_extension(&ext) {
                movie_discover_counter.fetch_add(1, Ordering::Relaxed);
                if movie_path_tx.send(entry.path().to_owned()).is_err() {
                    break;
                }
            }
        }
    });

    let mut movie_workers_join = Vec::with_capacity(config.movie_workers);
    for _ in 0..config.movie_workers {
        let worker_rx = movie_path_rx.clone();
        let worker_tx = movie_result_tx.clone();
        let worker_ingest_ts = movie_ingest_ts.clone();
        let worker_hash_downscale_size = config.hash_downscale_size;
        let worker_movie_frame_samples = config.movie_frame_samples;

        movie_workers_join.push(thread::spawn(move || {
            for path in worker_rx.iter() {
                let original_path = path.to_string_lossy().to_string();
                let extension = extension_of(&path);
                let detected = detect_video_format_from_path(&path);

                if matches!(detected, DetectedVideoFormat::Unknown) {
                    if worker_tx
                        .send(MovieProcessResult::Error {
                            path: original_path,
                            message: "unsupported movie format".to_string(),
                        })
                        .is_err()
                    {
                        break;
                    }
                    continue;
                }

                let output = match hash_video_perceptual(
                    &path,
                    worker_hash_downscale_size,
                    worker_movie_frame_samples,
                ) {
                    Ok(hash) => MovieProcessResult::Hashed {
                        hash,
                        original_path,
                        detected_format: detected.as_str().to_string(),
                        extension,
                        ingest_ts: worker_ingest_ts.clone(),
                    },
                    Err(message) => MovieProcessResult::Error {
                        path: original_path,
                        message,
                    },
                };

                if worker_tx.send(output).is_err() {
                    break;
                }
            }
        }));
    }
    drop(movie_result_tx);

    if dry_run {
        while let Ok(result) = movie_result_rx.recv() {
            movie_processed_total += 1;
            match result {
                MovieProcessResult::Hashed { .. } => {
                    movie_inserted_total += 1;
                }
                MovieProcessResult::Error { path, message } => {
                    errors += 1;
                    if console_errors_printed < config.max_console_errors {
                        eprintln!("Dry run movie error: {} (reason: {})", path, message);
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
            let mut insert_movie_stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO movie_hashes (
                    hash,
                    original_path,
                    detected_format,
                    extension,
                    ingest_ts
                )
                VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;

            while let Ok(result) = movie_result_rx.recv() {
                movie_processed_total += 1;
                match result {
                    MovieProcessResult::Hashed {
                        hash,
                        original_path,
                        detected_format,
                        extension,
                        ingest_ts,
                    } => {
                        let inserted = insert_movie_stmt.execute(params![
                            hash,
                            original_path,
                            detected_format,
                            extension,
                            ingest_ts
                        ])?;
                        if inserted > 0 {
                            movie_inserted_total += 1;
                        }
                    }
                    MovieProcessResult::Error { path, message } => {
                        errors += 1;
                        if console_errors_printed < config.max_console_errors {
                            eprintln!("Could not hash movie: {} (reason: {})", path, message);
                            console_errors_printed += 1;
                        } else {
                            console_errors_suppressed += 1;
                        }

                        if let Some(writer) = error_log_writer.as_mut() {
                            let _ = writeln!(writer, "path={}\taction=movie_error\treason={}", path, message);
                        }
                    }
                }
            }
        }
        tx.commit()?;
    }

    if movie_scanner.join().is_err() {
        eprintln!("Warning: movie scanner thread terminated unexpectedly");
    }
    for worker in movie_workers_join {
        if worker.join().is_err() {
            eprintln!("Warning: movie worker thread terminated unexpectedly");
        }
    }

    let movie_discovered_total = movie_discovered.load(Ordering::Relaxed);

    let elapsed = start.elapsed();
    println!("Done. Elapsed time: {:.2?}", elapsed);
    println!("Total images discovered: {}", total_discovered);
    println!("Total images processed: {}", processed_total);
    println!("Total movies discovered: {}", movie_discovered_total);
    println!("Total movies processed: {}", movie_processed_total);
    println!("Total errors: {}", errors);
    println!("Total transcoded to JPG: {}", transcoded_total);
    println!("Total quarantined: {}", quarantined_total);
    if dry_run {
        println!("Dry run preview: hashable files that would be inserted: {}", total_inserted);
        println!("Dry run preview: movie hashes that would be inserted: {}", movie_inserted_total);
    } else {
        println!("Total unique hashes inserted this run: {}", total_inserted);
        println!("Total unique movie hashes inserted this run: {}", movie_inserted_total);
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
