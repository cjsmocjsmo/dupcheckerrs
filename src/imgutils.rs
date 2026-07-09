use img_hash::image::{self, DynamicImage, ImageOutputFormat};
use img_hash::image::imageops::FilterType;
use img_hash::{HashAlg, HasherConfig};
use crossbeam_channel::Sender;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::{fs, io};
use turbojpeg;
use walkdir::WalkDir;

#[derive(Clone, Copy)]
pub enum DetectedFormat {
    Jpeg,
    Png,
    Gif,
    Webp,
    Bmp,
    Tiff,
    OtherImage,
    Unknown,
}

pub enum ProcessResult {
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

impl DetectedFormat {
    pub fn as_str(self) -> &'static str {
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

    pub fn is_transcode_candidate(self) -> bool {
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

pub fn detect_format_from_path(path: &Path) -> DetectedFormat {
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

pub fn is_image_extension(ext: &str) -> bool {
    matches!(ext, "jpg" | "jpeg" | "png" | "gif" | "webp")
}

pub fn transcode_to_jpeg(
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

pub fn preview_target_path(base_dir: &Path, source: &Path, extension: &str) -> PathBuf {
    unique_target_path(base_dir, source, extension)
}

pub fn quarantine_file(source: &Path, quarantine_dir: &Path) -> io::Result<PathBuf> {
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

pub fn process_image_path(
    path: &Path,
    dry_run: bool,
    transcode_dir: &Path,
    quarantine_dir: &Path,
    ingest_ts: &str,
    jpeg_quality: u8,
    hash_downscale_size: u32,
) -> ProcessResult {
    let hasher = HasherConfig::new().hash_alg(HashAlg::Mean).to_hasher();
    let original_path = path.to_string_lossy().to_string();
    let original_extension = extension_of(path);
    let detected = detect_format_from_path(path);

    match detected {
        DetectedFormat::Unknown => match if dry_run {
            let ext = path
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or("bin")
                .to_lowercase();
            Ok(preview_target_path(quarantine_dir, path, &ext))
        } else {
            quarantine_file(path, quarantine_dir)
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
                message: format!("unknown file signature and quarantine move failed: {}", e),
            },
        },
        DetectedFormat::Jpeg => match fs::read(path) {
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

                    let gray_image = image::GrayImage::from_raw(
                        width as u32,
                        height as u32,
                        grayscale,
                    )
                    .expect("grayscale buffer should match image dimensions");
                    let downscaled = image::imageops::resize(
                        &gray_image,
                        hash_downscale_size,
                        hash_downscale_size,
                        FilterType::Triangle,
                    );
                    let hash = hasher
                        .hash_image(&DynamicImage::ImageLuma8(downscaled))
                        .to_base64();
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
                        ingest_ts: ingest_ts.to_string(),
                    }
                }
                Err(e) => match if dry_run {
                    let ext = path
                        .extension()
                        .and_then(|x| x.to_str())
                        .unwrap_or("bin")
                        .to_lowercase();
                    Ok(preview_target_path(quarantine_dir, path, &ext))
                } else {
                    quarantine_file(path, quarantine_dir)
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
        },
        _ => match image::open(path) {
            Ok(img) => {
                let (stored_path, action_taken, transcode_path) = if detected.is_transcode_candidate() {
                    match if dry_run {
                        Ok(preview_target_path(transcode_dir, path, "jpg"))
                    } else {
                        transcode_to_jpeg(&img, path, transcode_dir, jpeg_quality)
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
                            return ProcessResult::Error {
                                path: original_path,
                                message: format!("failed to transcode image: {}", e),
                            };
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
                    ingest_ts: ingest_ts.to_string(),
                }
            }
            Err(e) => match if dry_run {
                let ext = path
                    .extension()
                    .and_then(|x| x.to_str())
                    .unwrap_or("bin")
                    .to_lowercase();
                Ok(preview_target_path(quarantine_dir, path, &ext))
            } else {
                quarantine_file(path, quarantine_dir)
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
    }
}

pub fn scan_image_paths(
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
        if is_image_extension(&ext) {
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
