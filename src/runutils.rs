use indicatif::ProgressBar;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::thread::JoinHandle;
use std::time::Instant;
use std::{env, fs};

pub fn env_flag(name: &str) -> bool {
    env::var(name)
        .ok()
        .map(|v| {
            let normalized = v.trim().to_ascii_lowercase();
            normalized == "1" || normalized == "true" || normalized == "yes" || normalized == "on"
        })
        .unwrap_or(false)
}

pub fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

pub fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

pub fn env_u8(name: &str, default: u8) -> u8 {
    env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<u8>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

pub fn env_u32(name: &str, default: u32) -> u32 {
    env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<u32>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

pub fn env_string(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

pub fn format_duration(total_secs: u64) -> String {
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;
    format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
}

pub fn log_image_heartbeat(
    start: Instant,
    now: Instant,
    last_heartbeat_at: &mut Instant,
    last_heartbeat_processed: &mut u64,
    discovered: u64,
    processed_total: u64,
    errors: u64,
    transcoded_total: u64,
    quarantined_total: u64,
    scan_done: bool,
    hash_downscale_size: u32,
    path_queue_len: usize,
    result_queue_len: usize,
    pb: &ProgressBar,
) {
    let elapsed_secs = start.elapsed().as_secs_f64();
    let avg_rate = if elapsed_secs > 0.0 {
        processed_total as f64 / elapsed_secs
    } else {
        0.0
    };
    let hb_delta_secs = now.duration_since(*last_heartbeat_at).as_secs_f64();
    let hb_delta_processed = processed_total.saturating_sub(*last_heartbeat_processed);
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
        hash_downscale_size,
        path_queue_len,
        result_queue_len
    );

    pb.set_message(format!(
        "discovered={} processed={} err={} rate={:.2}/s",
        discovered, processed_total, errors, avg_rate
    ));
    *last_heartbeat_at = now;
    *last_heartbeat_processed = processed_total;
}

pub fn log_movie_heartbeat(
    movie_phase_start: Instant,
    now: Instant,
    last_heartbeat_at: &mut Instant,
    last_heartbeat_processed: &mut u64,
    discovered: u64,
    processed_total: u64,
    errors: u64,
    inserted_total: u64,
    scan_done: bool,
    path_queue_len: usize,
    result_queue_len: usize,
    pb: &ProgressBar,
) {
    let elapsed_secs = movie_phase_start.elapsed().as_secs_f64();
    let avg_rate = if elapsed_secs > 0.0 {
        processed_total as f64 / elapsed_secs
    } else {
        0.0
    };
    let hb_delta_secs = now.duration_since(*last_heartbeat_at).as_secs_f64();
    let hb_delta_processed = processed_total.saturating_sub(*last_heartbeat_processed);
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
        "MOVIE HEARTBEAT elapsed={} discovered={} processed={} errors={} inserted={} avg_rate={:.2}/s inst_rate={:.2}/s scan_done={} pct={:.1}% eta={} q_path={} q_result={}",
        format_duration(movie_phase_start.elapsed().as_secs()),
        discovered,
        processed_total,
        errors,
        inserted_total,
        avg_rate,
        instant_rate,
        scan_done,
        pct,
        eta,
        path_queue_len,
        result_queue_len
    );

    pb.set_message(format!(
        "movies discovered={} processed={} err={} rate={:.2}/s",
        discovered, processed_total, errors, avg_rate
    ));
    *last_heartbeat_at = now;
    *last_heartbeat_processed = processed_total;
}

pub fn log_image_stall_warning(
    no_progress_secs: u64,
    discovered: u64,
    processed_total: u64,
    path_queue_len: usize,
    result_queue_len: usize,
    scan_done: bool,
) {
    eprintln!(
        "WARN stall detected: no progress for {}s (discovered={} processed={} q_path={} q_result={} scan_done={})",
        no_progress_secs,
        discovered,
        processed_total,
        path_queue_len,
        result_queue_len,
        scan_done
    );
}

pub fn log_movie_stall_warning(
    no_progress_secs: u64,
    discovered: u64,
    processed_total: u64,
    path_queue_len: usize,
    result_queue_len: usize,
    scan_done: bool,
) {
    eprintln!(
        "WARN movie stall detected: no progress for {}s (discovered={} processed={} q_path={} q_result={} scan_done={})",
        no_progress_secs,
        discovered,
        processed_total,
        path_queue_len,
        result_queue_len,
        scan_done
    );
}

pub fn log_console_limited(
    message: &str,
    console_errors_printed: &mut u64,
    console_errors_suppressed: &mut u64,
    max_console_errors: u64,
) {
    if *console_errors_printed < max_console_errors {
        eprintln!("{}", message);
        *console_errors_printed += 1;
    } else {
        *console_errors_suppressed += 1;
    }
}

pub fn write_optional_log_line(error_log_writer: &mut Option<BufWriter<fs::File>>, line: &str) {
    if let Some(writer) = error_log_writer.as_mut() {
        let _ = writeln!(writer, "{}", line);
    }
}

pub fn warn_if_join_failed<T>(handle: JoinHandle<T>, warning_message: &str) {
    if handle.join().is_err() {
        eprintln!("{}", warning_message);
    }
}

pub fn warn_if_any_join_failed<T>(handles: Vec<JoinHandle<T>>, warning_message: &str) {
    for handle in handles {
        if handle.join().is_err() {
            eprintln!("{}", warning_message);
        }
    }
}

pub fn print_run_summary(
    elapsed: std::time::Duration,
    total_images_discovered: u64,
    total_images_processed: u64,
    total_movies_discovered: u64,
    total_movies_processed: u64,
    total_errors: u64,
    total_transcoded: u64,
    total_quarantined: u64,
    dry_run: bool,
    total_hashes_inserted: u64,
    total_movie_hashes_inserted: u64,
    console_errors_suppressed: u64,
    error_log_path: &Path,
) {
    println!("Done. Elapsed time: {:.2?}", elapsed);
    println!("Total images discovered: {}", total_images_discovered);
    println!("Total images processed: {}", total_images_processed);
    println!("Total movies discovered: {}", total_movies_discovered);
    println!("Total movies processed: {}", total_movies_processed);
    println!("Total errors: {}", total_errors);
    println!("Total transcoded to JPG: {}", total_transcoded);
    println!("Total quarantined: {}", total_quarantined);
    if dry_run {
        println!(
            "Dry run preview: hashable files that would be inserted: {}",
            total_hashes_inserted
        );
        println!(
            "Dry run preview: movie hashes that would be inserted: {}",
            total_movie_hashes_inserted
        );
    } else {
        println!("Total unique hashes inserted this run: {}", total_hashes_inserted);
        println!(
            "Total unique movie hashes inserted this run: {}",
            total_movie_hashes_inserted
        );
    }

    if !dry_run && console_errors_suppressed > 0 {
        println!(
            "Suppressed {} additional image error logs on console. Full diagnostics: {}",
            console_errors_suppressed,
            error_log_path.display()
        );
    }
}
