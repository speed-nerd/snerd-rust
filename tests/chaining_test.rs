use snerd_rust::queue::SnerdQueue;
use snerd_rust::file_store::FileStore;
use snerd_rust::rate_limiter::RateLimiter;
use snerd_rust::task::RetryableTask;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn test_linear_job_chaining() {
    let log_path = "/tmp/snerd_test_chaining_linear_rs/tasks.log";
    let _ = std::fs::remove_dir_all("/tmp/snerd_test_chaining_linear_rs");

    let file_store = FileStore::new(&std::path::PathBuf::from(log_path)).unwrap();
    let rate_limiter = RateLimiter::new(&std::path::PathBuf::from("/tmp/snerd_test_chaining_linear_rs"));

    let mut pools = HashMap::new();
    pools.insert("default".to_string(), 10);

    let queue = SnerdQueue::new_with_pools("chain-linear-q", file_store, rate_limiter, pools);
    queue.start_processor(Duration::from_millis(50)).await;

    let events = Arc::new(Mutex::new(Vec::new()));
    let events_clone = events.clone();

    queue.register_task_handler("STEP_TASK", move |data: String| {
        events_clone.lock().unwrap().push(data);
        Ok(())
    }).await;

    let task_a = RetryableTask::new("taskA".to_string(), "STEP_TASK".to_string(), "A".to_string(), 3, 1.0, None, None, None, None, None, None, None, None, None, None);
    let task_b = RetryableTask::new("taskB".to_string(), "STEP_TASK".to_string(), "B".to_string(), 3, 1.0, None, None, None, None, None, None, None, None, None, Some(vec!["taskA".to_string()]));
    let task_c = RetryableTask::new("taskC".to_string(), "STEP_TASK".to_string(), "C".to_string(), 3, 1.0, None, None, None, None, None, None, None, None, None, Some(vec!["taskB".to_string()]));

    queue.enqueue(task_c).unwrap();
    queue.enqueue(task_a).unwrap();
    queue.enqueue(task_b).unwrap();

    // wait for completion
    for _ in 0..60 {
        sleep(Duration::from_millis(50)).await;
        if events.lock().unwrap().len() == 3 {
            break;
        }
    }

    let results = events.lock().unwrap().clone();
    assert_eq!(results.len(), 3, "Expected 3 tasks to run");
    assert_eq!(results[0], "A");
    assert_eq!(results[1], "B");
    assert_eq!(results[2], "C");
}

#[tokio::test]
async fn test_fanin_job_chaining() {
    let log_path = "/tmp/snerd_test_chaining_fanin_rs/tasks.log";
    let _ = std::fs::remove_dir_all("/tmp/snerd_test_chaining_fanin_rs");

    let file_store = FileStore::new(&std::path::PathBuf::from(log_path)).unwrap();
    let rate_limiter = RateLimiter::new(&std::path::PathBuf::from("/tmp/snerd_test_chaining_fanin_rs"));

    let mut pools = HashMap::new();
    pools.insert("default".to_string(), 10);

    let queue = SnerdQueue::new_with_pools("chain-fanin-q", file_store, rate_limiter, pools);
    queue.start_processor(Duration::from_millis(50)).await;

    let events = Arc::new(Mutex::new(Vec::new()));
    let events_clone = events.clone();

    queue.register_task_handler("STEP_TASK", move |data: String| {
        if data == "A" || data == "B" {
            std::thread::sleep(Duration::from_millis(100));
        }
        events_clone.lock().unwrap().push(data);
        Ok(())
    }).await;

    let task_a = RetryableTask::new("taskA".to_string(), "STEP_TASK".to_string(), "A".to_string(), 3, 1.0, None, None, None, None, None, None, None, None, None, None);
    let task_b = RetryableTask::new("taskB".to_string(), "STEP_TASK".to_string(), "B".to_string(), 3, 1.0, None, None, None, None, None, None, None, None, None, None);
    let task_c = RetryableTask::new("taskC".to_string(), "STEP_TASK".to_string(), "C".to_string(), 3, 1.0, None, None, None, None, None, None, None, None, None, Some(vec!["taskA".to_string(), "taskB".to_string()]));

    queue.enqueue(task_c).unwrap();
    queue.enqueue(task_a).unwrap();
    queue.enqueue(task_b).unwrap();

    // wait for completion
    for _ in 0..60 {
        sleep(Duration::from_millis(50)).await;
        if events.lock().unwrap().len() == 3 {
            break;
        }
    }

    let results = events.lock().unwrap().clone();
    assert_eq!(results.len(), 3, "Expected 3 tasks to run");
    assert_eq!(results[2], "C", "C must run last");
}

#[tokio::test]
async fn test_orphaned_job_chaining() {
    let log_path = "/tmp/snerd_test_chaining_orphan_rs/tasks.log";
    let _ = std::fs::remove_dir_all("/tmp/snerd_test_chaining_orphan_rs");

    let file_store = FileStore::new(&std::path::PathBuf::from(log_path)).unwrap();
    let rate_limiter = RateLimiter::new(&std::path::PathBuf::from("/tmp/snerd_test_chaining_orphan_rs"));

    let mut pools = HashMap::new();
    pools.insert("default".to_string(), 10);

    let queue = SnerdQueue::new_with_pools("chain-orphan-q", file_store, rate_limiter, pools);
    queue.start_processor(Duration::from_millis(50)).await;

    let events = Arc::new(Mutex::new(Vec::new()));
    let events_clone = events.clone();

    queue.register_task_handler("STEP_TASK", move |data: String| {
        events_clone.lock().unwrap().push(data);
        Ok(())
    }).await;

    let task_a = RetryableTask::new("taskA".to_string(), "STEP_TASK".to_string(), "A".to_string(), 3, 1.0, None, None, None, None, None, None, None, None, None, Some(vec!["nonexistent".to_string()]));
    queue.enqueue(task_a).unwrap();

    sleep(Duration::from_millis(500)).await;

    let results = events.lock().unwrap().clone();
    assert_eq!(results.len(), 1, "Orphaned task should execute because absent parent is treated as completed in v1");
}
