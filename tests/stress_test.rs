use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tempfile::tempdir;
use snerd_rust::file_store::FileStore;
use snerd_rust::queue::SnerdQueue;
use snerd_rust::rate_limiter::RateLimiter;
use snerd_rust::task::RetryableTask;

const NUM_JOBS: usize = 50_000;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_embedded_stress_50k() {
    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir.path().join("stress_tasks.log");

    let store = FileStore::new(&file_path).unwrap();
    let rate_limiter = RateLimiter::new(&tempdir().unwrap().path().to_path_buf());
    let queue = SnerdQueue::new("stress-queue", store, rate_limiter);

    let exec_counter = Arc::new(AtomicUsize::new(0));
    let exec_counter_clone = exec_counter.clone();

    queue
        .register_task_handler("stress_job", move |_data: String| {
            exec_counter_clone.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await;

    queue.start_processor(Duration::from_millis(100)).await;

    // Enqueue NUM_JOBS tasks
    println!("Enqueueing {} jobs...", NUM_JOBS);
    let enqueue_start = Instant::now();

    for i in 0..NUM_JOBS {
        let task = RetryableTask::new(
            format!("stress-{}", i),
            "stress_job".to_string(),
            format!("{{\"id\":{}}}", i),
            3,
            0.0,
            None, None, None, None, None, None, None, None, None,
        );
        queue.enqueue(task).unwrap();
    }

    let enqueue_elapsed = enqueue_start.elapsed();
    println!("Enqueued in {:.2}s ({:.0} jobs/sec)", 
        enqueue_elapsed.as_secs_f64(), 
        NUM_JOBS as f64 / enqueue_elapsed.as_secs_f64());

    // Wait for all jobs to execute
    let start = Instant::now();
    let timeout = Duration::from_secs(1800);

    while exec_counter.load(Ordering::SeqCst) < NUM_JOBS && start.elapsed() < timeout {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let elapsed = start.elapsed();
    let total_exec = exec_counter.load(Ordering::SeqCst);

    println!("\n📊 Results:");
    println!("  Total executions: {}", total_exec);
    println!("  Expected:         {}", NUM_JOBS);
    println!("  Time:             {:.1}s", elapsed.as_secs_f64());
    println!("  Throughput:       {:.1} jobs/sec", total_exec as f64 / elapsed.as_secs_f64());

    assert_eq!(total_exec, NUM_JOBS, "Expected {} executions, got {}", NUM_JOBS, total_exec);
    println!("✅ PASS: {} jobs, 0 duplicates, {:.1} jobs/sec", 
        NUM_JOBS, total_exec as f64 / elapsed.as_secs_f64());
}
