use snerd_rust::file_store::FileStore;
use snerd_rust::queue::SnerdQueue;
use snerd_rust::rate_limiter::RateLimiter;
use snerd_rust::task::RetryableTask;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[tokio::test]
async fn test_rate_limiting() {
    let test_dir = std::env::temp_dir().join("snerd_test_rate_limit");
    let _ = std::fs::remove_dir_all(&test_dir);
    std::fs::create_dir_all(&test_dir).unwrap();
    let tasks_log = test_dir.join("tasks.log");

    let file_store = FileStore::new(&tasks_log).unwrap();
    let rate_limiter = RateLimiter::new(&tasks_log);
    let queue = Arc::new(SnerdQueue::new("test-queue", file_store.clone(), rate_limiter));

    let execution_count = Arc::new(Mutex::new(0));
    let count_clone = execution_count.clone();

    queue
        .register_task_handler("ai_gen", move |_data| {
            let mut count = count_clone.lock().unwrap();
            *count += 1;
            Ok(())
        })
        .await;

    queue.start_processor(Duration::from_millis(100)).await;

    for i in 0..5 {
        let task = RetryableTask::new(
            format!("task-{}", i),
            "ai_gen".to_string(),
            "{}".to_string(),
            3,
            1.0,
            Some("openai_api".to_string()), // rateLimitGroup
            Some(2),                        // maxPerMinute (2 per minute)
            None,
            None,
            None,
            None,
            None,
            None, // max_execution_seconds
            None, // pool
        );
        queue.enqueue(task).unwrap();
    }

    tokio::time::sleep(Duration::from_secs(1)).await;

    let executed = *execution_count.lock().unwrap();
    assert_eq!(executed, 2, "Expected exactly 2 executions due to rate limit, but got {}", executed);
    
    println!("SUCCESS! Enqueued 5 tasks, but Rate Limiter successfully throttled executions to {}", executed);

    let mut pending = 0;
    for task in file_store.read_tasks().unwrap() {
        if task.deleted_at.is_none() {
            pending += 1;
        }
    }
    assert_eq!(pending, 3, "Expected 3 tasks still pending in the queue");

    let _ = std::fs::remove_dir_all(&test_dir);
}
