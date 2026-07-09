mod config;
mod dbutils;
mod imgutils;
mod movutils;
mod runutils;

use rusqlite::{params, Result};
use crossbeam_channel::{bounded, RecvTimeoutError};
use indicatif::{ProgressBar, ProgressStyle};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use std::{env, fs};
use time::OffsetDateTime;
use crate::config::RuntimeConfig;
use crate::dbutils::open_database;
use crate::imgutils::{process_image_path, scan_image_paths, ProcessResult};
use crate::movutils::{process_movie_path, scan_movie_paths, MovieProcessResult};
use crate::runutils::{
    log_console_limited, log_image_heartbeat, log_image_stall_warning, log_movie_heartbeat,
    log_movie_stall_warning, print_run_summary, warn_if_any_join_failed, warn_if_join_failed,
    write_optional_log_line,
};

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
        config.env_overrides_enabled
    );

    let mut conn = open_database(dry_run, &config.db_path)?;

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
        scan_image_paths(search_dir, path_tx, discover_counter, scanner_done_flag);
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
            for path in worker_rx.iter() {
                let output = process_image_path(
                    &path,
                    dry_run,
                    &worker_transcode_dir,
                    &worker_quarantine_dir,
                    &worker_ingest_ts,
                    worker_jpeg_quality,
                    worker_hash_downscale_size,
                );

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
                            log_console_limited(
                                &format!(
                                    "Dry run: {} (detected={}, reason={}, target={})",
                                    path, detected_format, message, quarantine_path
                                ),
                                &mut console_errors_printed,
                                &mut console_errors_suppressed,
                                config.max_console_errors,
                            );
                        }
                        ProcessResult::Error { path, message } => {
                            errors += 1;
                            log_console_limited(
                                &format!("Dry run error: {} (reason: {})", path, message),
                                &mut console_errors_printed,
                                &mut console_errors_suppressed,
                                config.max_console_errors,
                            );
                        }
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }

            let now = Instant::now();
            if now.duration_since(last_heartbeat_at) >= heartbeat_interval {
                log_image_heartbeat(
                    start,
                    now,
                    &mut last_heartbeat_at,
                    &mut last_heartbeat_processed,
                    discovered_count.load(Ordering::Relaxed),
                    processed_total,
                    errors,
                    transcoded_total,
                    quarantined_total,
                    scanner_done.load(Ordering::Relaxed),
                    config.hash_downscale_size,
                    path_rx.len(),
                    result_rx.len(),
                    &pb,
                );
            }

            if now.duration_since(last_progress_at) >= stall_warn_interval
                && now.duration_since(last_stall_warn_at) >= heartbeat_interval
            {
                log_image_stall_warning(
                    now.duration_since(last_progress_at).as_secs(),
                    discovered_count.load(Ordering::Relaxed),
                    processed_total,
                    path_rx.len(),
                    result_rx.len(),
                    scanner_done.load(Ordering::Relaxed),
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
                                log_console_limited(
                                    &format!(
                                        "Quarantined file: {} (detected={}, reason={}, quarantine={})",
                                        path, detected_format, message, quarantine_path
                                    ),
                                    &mut console_errors_printed,
                                    &mut console_errors_suppressed,
                                    config.max_console_errors,
                                );

                                write_optional_log_line(
                                    &mut error_log_writer,
                                    &format!(
                                        "path={}\tdetected={}\taction=quarantined\treason={}\tquarantine_path={}",
                                        path, detected_format, message, quarantine_path
                                    ),
                                );
                            }
                            ProcessResult::Error { path, message } => {
                                errors += 1;
                                log_console_limited(
                                    &format!("Could not open image: {} (reason: {})", path, message),
                                    &mut console_errors_printed,
                                    &mut console_errors_suppressed,
                                    config.max_console_errors,
                                );

                                write_optional_log_line(
                                    &mut error_log_writer,
                                    &format!("path={}\taction=error\treason={}", path, message),
                                );
                            }
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => break,
                }

                let now = Instant::now();
                if now.duration_since(last_heartbeat_at) >= heartbeat_interval {
                    log_image_heartbeat(
                        start,
                        now,
                        &mut last_heartbeat_at,
                        &mut last_heartbeat_processed,
                        discovered_count.load(Ordering::Relaxed),
                        processed_total,
                        errors,
                        transcoded_total,
                        quarantined_total,
                        scanner_done.load(Ordering::Relaxed),
                        config.hash_downscale_size,
                        path_rx.len(),
                        result_rx.len(),
                        &pb,
                    );
                }

                if now.duration_since(last_progress_at) >= stall_warn_interval
                    && now.duration_since(last_stall_warn_at) >= heartbeat_interval
                {
                    log_image_stall_warning(
                        now.duration_since(last_progress_at).as_secs(),
                        discovered_count.load(Ordering::Relaxed),
                        processed_total,
                        path_rx.len(),
                        result_rx.len(),
                        scanner_done.load(Ordering::Relaxed),
                    );
                    last_stall_warn_at = now;
                }
            }
        }
        tx.commit()?;
    }

    drop(path_rx);

    warn_if_join_failed(scanner, "Warning: scanner thread terminated unexpectedly");
    warn_if_any_join_failed(workers_join, "Warning: worker thread terminated unexpectedly");

    let total_discovered = discovered_count.load(Ordering::Relaxed);
    pb.set_message(format!("discovered={} processed={}", total_discovered, processed_total));
    pb.finish_with_message("Processing done");

    println!("Image phase complete. Starting movie phase...");
    let movie_phase_start = Instant::now();
    let movie_ingest_ts = OffsetDateTime::now_utc().unix_timestamp().to_string();
    let movie_discovered = Arc::new(AtomicU64::new(0));
    let movie_scanner_done = Arc::new(AtomicBool::new(false));

    let (movie_path_tx, movie_path_rx) = bounded::<PathBuf>(config.movie_path_queue_cap);
    let (movie_result_tx, movie_result_rx) = bounded::<MovieProcessResult>(config.movie_result_queue_cap);

    let movie_search_dir = config.search_dir.clone();
    let movie_discover_counter = Arc::clone(&movie_discovered);
    let movie_scanner_done_flag = Arc::clone(&movie_scanner_done);
    let movie_scanner = thread::spawn(move || {
        scan_movie_paths(
            movie_search_dir,
            movie_path_tx,
            movie_discover_counter,
            movie_scanner_done_flag,
        );
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
                let output = process_movie_path(
                    &path,
                    worker_hash_downscale_size,
                    worker_movie_frame_samples,
                    &worker_ingest_ts,
                );

                if worker_tx.send(output).is_err() {
                    break;
                }
            }
        }));
    }
    drop(movie_result_tx);

    let mut movie_last_heartbeat_at = Instant::now();
    let mut movie_last_heartbeat_processed = 0u64;
    let mut movie_last_progress_at = Instant::now();
    let mut movie_last_stall_warn_at = Instant::now();

    if dry_run {
        loop {
            match movie_result_rx.recv_timeout(Duration::from_millis(500)) {
                Ok(result) => {
                    movie_processed_total += 1;
                    movie_last_progress_at = Instant::now();
                    match result {
                        MovieProcessResult::Hashed { .. } => {
                            movie_inserted_total += 1;
                        }
                        MovieProcessResult::Error { path, message } => {
                            errors += 1;
                            log_console_limited(
                                &format!("Dry run movie error: {} (reason: {})", path, message),
                                &mut console_errors_printed,
                                &mut console_errors_suppressed,
                                config.max_console_errors,
                            );
                        }
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }

            let now = Instant::now();
            if now.duration_since(movie_last_heartbeat_at) >= heartbeat_interval {
                log_movie_heartbeat(
                    movie_phase_start,
                    now,
                    &mut movie_last_heartbeat_at,
                    &mut movie_last_heartbeat_processed,
                    movie_discovered.load(Ordering::Relaxed),
                    movie_processed_total,
                    errors,
                    movie_inserted_total,
                    movie_scanner_done.load(Ordering::Relaxed),
                    movie_path_rx.len(),
                    movie_result_rx.len(),
                    &pb,
                );
            }

            if now.duration_since(movie_last_progress_at) >= stall_warn_interval
                && now.duration_since(movie_last_stall_warn_at) >= heartbeat_interval
            {
                log_movie_stall_warning(
                    now.duration_since(movie_last_progress_at).as_secs(),
                    movie_discovered.load(Ordering::Relaxed),
                    movie_processed_total,
                    movie_path_rx.len(),
                    movie_result_rx.len(),
                    movie_scanner_done.load(Ordering::Relaxed),
                );
                movie_last_stall_warn_at = now;
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

            loop {
                match movie_result_rx.recv_timeout(Duration::from_millis(500)) {
                    Ok(result) => {
                        movie_processed_total += 1;
                        movie_last_progress_at = Instant::now();
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
                                log_console_limited(
                                    &format!("Could not hash movie: {} (reason: {})", path, message),
                                    &mut console_errors_printed,
                                    &mut console_errors_suppressed,
                                    config.max_console_errors,
                                );

                                write_optional_log_line(
                                    &mut error_log_writer,
                                    &format!("path={}\taction=movie_error\treason={}", path, message),
                                );
                            }
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => break,
                }

                let now = Instant::now();
                if now.duration_since(movie_last_heartbeat_at) >= heartbeat_interval {
                    log_movie_heartbeat(
                        movie_phase_start,
                        now,
                        &mut movie_last_heartbeat_at,
                        &mut movie_last_heartbeat_processed,
                        movie_discovered.load(Ordering::Relaxed),
                        movie_processed_total,
                        errors,
                        movie_inserted_total,
                        movie_scanner_done.load(Ordering::Relaxed),
                        movie_path_rx.len(),
                        movie_result_rx.len(),
                        &pb,
                    );
                }

                if now.duration_since(movie_last_progress_at) >= stall_warn_interval
                    && now.duration_since(movie_last_stall_warn_at) >= heartbeat_interval
                {
                    log_movie_stall_warning(
                        now.duration_since(movie_last_progress_at).as_secs(),
                        movie_discovered.load(Ordering::Relaxed),
                        movie_processed_total,
                        movie_path_rx.len(),
                        movie_result_rx.len(),
                        movie_scanner_done.load(Ordering::Relaxed),
                    );
                    movie_last_stall_warn_at = now;
                }
            }
        }
        tx.commit()?;
    }

    warn_if_join_failed(movie_scanner, "Warning: movie scanner thread terminated unexpectedly");
    warn_if_any_join_failed(
        movie_workers_join,
        "Warning: movie worker thread terminated unexpectedly",
    );

    let movie_discovered_total = movie_discovered.load(Ordering::Relaxed);

    let elapsed = start.elapsed();
    print_run_summary(
        elapsed,
        total_discovered,
        processed_total,
        movie_discovered_total,
        movie_processed_total,
        errors,
        transcoded_total,
        quarantined_total,
        dry_run,
        total_inserted,
        movie_inserted_total,
        console_errors_suppressed,
        &error_log_path,
    );

    if let Some(mut writer) = error_log_writer {
        let _ = writer.flush();
    }

    Ok(())
}
