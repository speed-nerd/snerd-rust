use snerd_rust::queue::SnerdQueue;
use snerd_rust::file_store::FileStore;
use snerd_rust::rate_limiter::RateLimiter;
use snerd_rust::task::RetryableTask;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn test_worker_pools_isolation() {
    let log_path = "/tmp/snerd_test_pools_rs/tasks.log";
    let _ = std::fs::remove_dir_all("/tmp/snerd_test_pools_rs");

    let file_store = FileStore::new(&std::path::PathBuf::from(log_path)).unwrap();
    let rate_limiter = RateLimiter::new(&std::path::PathBuf::from("/tmp/snerd_test_pools_rs"));

    let mut pools = HashMap::new();
    pools.insert("default".to_string(), 1);
    pools.insert("urgent".to_string(), 1);

    let queue = SnerdQueue::new_with_pools("test-pools-q", file_store, rate_limiter, pools);
    queue.start_processor(Duration::from_millis(100)).await;

    let default_started = Arc::new(Notify::new());
    let default_started_clone = default_started.clone();

    let urgent_done = Arc::new(Notify::new());
    let urgent_done_clone = urgent_done.clone();

    queue.register_task_handler("LONG_TASK", move |data: String| {
        let default_started = default_started_clone.clone();
        let urgent_done = urgent_done_clone.clone();
        
        if data == "default" {
            default_started.notify_one();
            // Block the default pool permit for a long time
            std::thread::sleep(Duration::from_secs(3));
        } else if data == "urgent" {
            urgent_done.notify_one();
        }
        Ok(())
    }).await;

    // Enqueue long-running default task
    let mut default_task = RetryableTask::new(
        "t-def-1".to_string(),
        "LONG_TASK".to_string(),
        "default".to_string(),
        1,
        0.0,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    );
    queue.enqueue(default_task).unwrap();

    // Wait for the default task to start executing
    tokio::select! {
        _ = default_started.notified() => {}
        _ = sleep(Duration::from_secs(2)) => {
            panic!("Timeout waiting for default task to start");
        }
    }

    // Enqueue urgent task with pool="urgent"
    let mut urgent_task = RetryableTask::new(
        "t-urg-1".to_string(),
        "LONG_TASK".to_string(),
        "urgent".to_string(),
        1,
        0.0,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some("urgent".to_string()),
    );
    queue.enqueue(urgent_task).unwrap();

    // Assert urgent task executes immediately despite default pool being blocked
    tokio::select! {
        _ = urgent_done.notified() => {
            // Success!
        }
        _ = sleep(Duration::from_secs(2)) => {
            panic!("Urgent task was blocked by default pool! Isolation failed.");
        }
    }
}
