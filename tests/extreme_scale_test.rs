use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tempfile::tempdir;
use snerd_rust::file_store::FileStore;
use snerd_rust::queue::SnerdQueue;
use snerd_rust::rate_limiter::RateLimiter;
use snerd_rust::task::RetryableTask;

/// Get current RSS memory usage in KB (macOS)
fn get_memory_kb() -> u64 {
    if let Ok(output) = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
    {
        let s = String::from_utf8_lossy(&output.stdout).trim().to_string();
        s.parse().unwrap_or(0)
    } else {
        0
    }
}

/// Get disk usage of a directory in KB
fn get_disk_kb(path: &std::path::Path) -> u64 {
    if let Ok(output) = std::process::Command::new("du")
        .args(["-sk", path.to_str().unwrap()])
        .output()
    {
        let s = String::from_utf8_lossy(&output.stdout);
        s.split_whitespace().next()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    } else {
        0
    }
}

async fn run_scale_test(num_jobs: usize, label: &str) -> bool {
    println!("\n{}", "=".repeat(60));
    println!("🔥 EXTREME SCALE TEST: {} ({} tasks)", label, num_jobs);
    println!("{}", "=".repeat(60));

    let temp_dir = tempdir().unwrap();
    let temp_path = temp_dir.path().to_path_buf();
    let file_path = temp_path.join("tasks").join("tasks.log");
    std::fs::create_dir_all(temp_path.join("tasks")).unwrap();

    let store = FileStore::new(&file_path).unwrap();
    let rate_limiter = RateLimiter::new(&temp_path);
    let queue = Arc::new(SnerdQueue::new("extreme-queue", store, rate_limiter));

    let exec_counter = Arc::new(AtomicUsize::new(0));
    let dup_counter = Arc::new(AtomicUsize::new(0));
    let exec_counter_clone = exec_counter.clone();
    let dup_counter_clone = dup_counter.clone();

    // Track seen task IDs to detect duplicates
    let seen = Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
    let seen_clone = seen.clone();

    queue
        .register_task_handler("extreme_job", move |data: String| {
            let mut s = seen_clone.lock().unwrap();
            if !s.insert(data.clone()) {
                dup_counter_clone.fetch_add(1, Ordering::SeqCst);
            }
            exec_counter_clone.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await;

    queue.start_processor(Duration::from_millis(50)).await;

    let mem_before = get_memory_kb();
    println!("📊 Memory before enqueue: {} MB", mem_before / 1024);

    // === ENQUEUE PHASE ===
    println!("\n📥 Enqueueing {} tasks...", num_jobs);
    let enqueue_start = Instant::now();
    let mut last_report = Instant::now();

    for i in 0..num_jobs {
        let task = RetryableTask::new(
            format!("extreme-{}", i),
            "extreme_job".to_string(),
            format!("{}", i),  // simple data = task index
            3,
            0.0,
            None, None, None, None, None, None, None, None, None,
        );
        queue.enqueue(task).unwrap();

        // Report progress every 5 seconds
        if last_report.elapsed() > Duration::from_secs(5) {
            let elapsed = enqueue_start.elapsed();
            let rate = (i + 1) as f64 / elapsed.as_secs_f64();
            let mem = get_memory_kb();
            let disk = get_disk_kb(&temp_path);
            println!("  [{}/{}] {:.0} tasks/sec | mem: {} MB | disk: {} MB",
                i + 1, num_jobs, rate, mem / 1024, disk / 1024);
            last_report = Instant::now();
        }
    }

    let enqueue_elapsed = enqueue_start.elapsed();
    let enqueue_rate = num_jobs as f64 / enqueue_elapsed.as_secs_f64();
    let mem_after_enqueue = get_memory_kb();
    let disk_after_enqueue = get_disk_kb(&temp_path);

    println!("\n✅ Enqueue complete:");
    println!("  Time:       {:.2}s", enqueue_elapsed.as_secs_f64());
    println!("  Throughput: {:.0} tasks/sec", enqueue_rate);
    println!("  Memory:     {} MB", mem_after_enqueue / 1024);
    println!("  Disk:       {} MB", disk_after_enqueue / 1024);

    // === PROCESSING PHASE ===
    println!("\n⚙️  Waiting for processing...");
    let process_start = Instant::now();
    // Timeout: generous — 100 tasks/sec minimum expected
    let timeout_secs = std::cmp::max(300, (num_jobs / 50) as u64);
    let timeout = Duration::from_secs(timeout_secs);
    let mut last_report = Instant::now();

    while exec_counter.load(Ordering::SeqCst) < num_jobs && process_start.elapsed() < timeout {
        tokio::time::sleep(Duration::from_millis(200)).await;

        if last_report.elapsed() > Duration::from_secs(10) {
            let done = exec_counter.load(Ordering::SeqCst);
            let elapsed = process_start.elapsed().as_secs_f64();
            let rate = done as f64 / elapsed;
            let mem = get_memory_kb();
            let disk = get_disk_kb(&temp_path);
            let dups = dup_counter.load(Ordering::SeqCst);
            println!("  [{}/{}] {:.0} tasks/sec | mem: {} MB | disk: {} MB | dups: {} | ETA: {:.0}s",
                done, num_jobs, rate, mem / 1024, disk / 1024, dups,
                if rate > 0.0 { (num_jobs - done) as f64 / rate } else { 0.0 });
            last_report = Instant::now();
        }
    }

    let process_elapsed = process_start.elapsed();
    let total_exec = exec_counter.load(Ordering::SeqCst);
    let total_dups = dup_counter.load(Ordering::SeqCst);
    let process_rate = total_exec as f64 / process_elapsed.as_secs_f64();
    let mem_final = get_memory_kb();
    let disk_final = get_disk_kb(&temp_path);

    // === RESULTS ===
    println!("\n{}", "=".repeat(60));
    println!("📊 RESULTS: {} ({} tasks)", label, num_jobs);
    println!("{}", "=".repeat(60));
    println!("  Enqueue:");
    println!("    Time:       {:.2}s", enqueue_elapsed.as_secs_f64());
    println!("    Throughput: {:.0} tasks/sec", enqueue_rate);
    println!("  Processing:");
    println!("    Time:       {:.2}s", process_elapsed.as_secs_f64());
    println!("    Throughput: {:.0} tasks/sec", process_rate);
    println!("  Integrity:");
    println!("    Executed:   {} / {}", total_exec, num_jobs);
    println!("    Duplicates: {}", total_dups);
    println!("  Resources:");
    println!("    Memory:     {} MB → {} MB → {} MB (before/enqueue/done)",
        mem_before / 1024, mem_after_enqueue / 1024, mem_final / 1024);
    println!("    Disk:       {} MB (final)", disk_final / 1024);

    let passed = total_exec == num_jobs && total_dups == 0;
    if passed {
        println!("  ✅ PASS");
    } else {
        println!("  ❌ FAIL (exec={} dups={})", total_exec, total_dups);
    }

    // Clean up temp dir to save disk space for next test
    drop(queue);
    let _ = std::fs::remove_dir_all(&temp_path);

    passed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_extreme_scale_100k() {
    let passed = run_scale_test(100_000, "100K").await;
    assert!(passed, "100K test failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_extreme_scale_250k() {
    let passed = run_scale_test(250_000, "250K").await;
    assert!(passed, "250K test failed");
}
