use ffmpeg_light::generate_thumbnail;
use ffmpeg_light::probe;
use ffmpeg_light::thumbnail::{ThumbnailFormat, ThumbnailOptions};
use ffmpeg_light::types::Time;
use img_hash::image::{self, DynamicImage};
use img_hash::image::imageops::FilterType;
use img_hash::{HashAlg, HasherConfig};
use crossbeam_channel::Sender;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::{env, fs};
use time::OffsetDateTime;
use walkdir::WalkDir;

#[derive(Clone, Copy)]
pub enum DetectedVideoFormat {
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

pub enum MovieProcessResult {
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

impl DetectedVideoFormat {
    pub fn as_str(self) -> &'static str {
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

pub fn is_movie_extension(ext: &str) -> bool {
    matches!(ext, "mp4" | "mov" | "m4v" | "mkv" | "webm" | "avi" | "mpg" | "mpeg")
}

pub fn detect_video_format_from_path(path: &Path) -> DetectedVideoFormat {
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

pub fn hash_video_perceptual(
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
            "movie_{}_{}_{}_{}.png",
            source_id,
            unique_run,
            std::process::id(),
            idx
        ));

        let thumb_opts = ThumbnailOptions::new(frame_time)
            .size(target_size, target_size)
            .format(ThumbnailFormat::Png);

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
                hasher.hash_image(&DynamicImage::ImageLuma8(gray)).to_base64()
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

pub fn process_movie_path(
    path: &Path,
    hash_downscale_size: u32,
    frame_samples: usize,
    ingest_ts: &str,
) -> MovieProcessResult {
    let original_path = path.to_string_lossy().to_string();
    let extension = extension_of(path);
    let detected = detect_video_format_from_path(path);

    if matches!(detected, DetectedVideoFormat::Unknown) {
        return MovieProcessResult::Error {
            path: original_path,
            message: "unsupported movie format".to_string(),
        };
    }

    match hash_video_perceptual(path, hash_downscale_size, frame_samples) {
        Ok(hash) => MovieProcessResult::Hashed {
            hash,
            original_path,
            detected_format: detected.as_str().to_string(),
            extension,
            ingest_ts: ingest_ts.to_string(),
        },
        Err(message) => MovieProcessResult::Error {
            path: original_path,
            message,
        },
    }
}

pub fn scan_movie_paths(
    search_dir: String,
    path_tx: Sender<PathBuf>,
    discover_counter: Arc<AtomicU64>,
    scanner_done: Arc<AtomicBool>,
) {
    for entry in WalkDir::new(search_dir).into_iter().filter_map(|e| e.ok()) {
        if !entry.path().is_file() {
            continue;
        }
        let ext = extension_of(entry.path());
        if is_movie_extension(&ext) {
            discover_counter.fetch_add(1, Ordering::Relaxed);
            if path_tx.send(entry.path().to_owned()).is_err() {
                break;
            }
        }
    }
    scanner_done.store(true, Ordering::Relaxed);
}

fn extension_of(path: &Path) -> String {
    path.extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase()
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

fn path_fingerprint(path: &Path) -> u64 {
    let mut hasher = DefaultHasher::new();
    path.to_string_lossy().hash(&mut hasher);
    hasher.finish()
}
