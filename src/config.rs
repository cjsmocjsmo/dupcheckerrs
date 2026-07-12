use crate::runutils::{env_flag, env_string, env_u32, env_u64, env_u8, env_usize};
use std::env;

// Set all runtime behavior here, then build on rpi4 and copy only the binary to rpi3b+.
const DUPCHECKER_DRY_RUN: bool = false;
const DUPCHECKER_WORKERS: usize = 3;
const DUPCHECKER_HEARTBEAT_SECS: u64 = 15;
const DUPCHECKER_STALL_WARN_SECS: u64 = 120;
const DUPCHECKER_PROGRESS_ENABLED: bool = true;
const DUPCHECKER_DB_PATH: &str = "/media/PiTB/images.db";
const DUPCHECKER_SEARCH_DIR: &str = "/media/PiTB/foofuck2";
const DUPCHECKER_ERROR_LOG_FILE: &str = "dupcheckerrs-errors.log";
const DUPCHECKER_TRANSCODE_DIR_NAME: &str = "transcoded_jpg";
const DUPCHECKER_QUARANTINE_DIR_NAME: &str = "quarantine";
const DUPCHECKER_MAX_CONSOLE_ERRORS: u64 = 20;
const DUPCHECKER_PATH_QUEUE_CAP: usize = 2048;
const DUPCHECKER_RESULT_QUEUE_CAP: usize = 2048;
const DUPCHECKER_JPEG_QUALITY: u8 = 95;
const DUPCHECKER_HASH_DOWNSCALE_SIZE: u32 = 128;
const DUPCHECKER_MOVIE_WORKERS: usize = 1;
const DUPCHECKER_MOVIE_FRAME_SAMPLES: usize = 5;
const DUPCHECKER_MOVIE_PATH_QUEUE_CAP: usize = 512;
const DUPCHECKER_MOVIE_RESULT_QUEUE_CAP: usize = 512;
const DUPCHECKER_MASTER_IMAGE_DIR: &str = "/media/PiTB/RustMasterPics";
const DUPCHECKER_MASTER_MOVIE_DIR: &str = "/media/PiTB/RustMasterMovies";

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
const ENV_MASTER_IMAGE_DIR: &str = "DUPCHECKER_MASTER_IMAGE_DIR";
const ENV_MASTER_MOVIE_DIR: &str = "DUPCHECKER_MASTER_MOVIE_DIR";

pub struct RuntimeConfig {
    pub dry_run: bool,
    pub workers: usize,
    pub heartbeat_secs: u64,
    pub stall_warn_secs: u64,
    pub progress_enabled: bool,
    pub db_path: String,
    pub search_dir: String,
    pub error_log_file: String,
    pub transcode_dir_name: String,
    pub quarantine_dir_name: String,
    pub max_console_errors: u64,
    pub path_queue_cap: usize,
    pub result_queue_cap: usize,
    pub jpeg_quality: u8,
    pub hash_downscale_size: u32,
    pub movie_workers: usize,
    pub movie_frame_samples: usize,
    pub movie_path_queue_cap: usize,
    pub movie_result_queue_cap: usize,
    pub master_image_dir: String,
    pub master_movie_dir: String,
    pub env_overrides_enabled: bool,
}

impl RuntimeConfig {
    pub fn load() -> Self {
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
            master_image_dir: DUPCHECKER_MASTER_IMAGE_DIR.to_string(),
            master_movie_dir: DUPCHECKER_MASTER_MOVIE_DIR.to_string(),
            env_overrides_enabled: ENABLE_ENV_OVERRIDES,
        };

        let mut loaded = if ENABLE_ENV_OVERRIDES {
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
                master_image_dir: base.master_image_dir.clone(),
                master_movie_dir: base.master_movie_dir.clone(),
                env_overrides_enabled: ENABLE_ENV_OVERRIDES,
            }
        } else {
            base
        };

        // Copy phases are opt-in: destination dirs are set only when env vars are present and non-empty.
        loaded.master_image_dir = env::var(ENV_MASTER_IMAGE_DIR)
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| loaded.master_image_dir.trim().to_string());
        loaded.master_movie_dir = env::var(ENV_MASTER_MOVIE_DIR)
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| loaded.master_movie_dir.trim().to_string());

        loaded
    }
}
