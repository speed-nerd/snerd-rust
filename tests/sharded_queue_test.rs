use snerd_rust::sharded_queue::SnerdShardedQueue;
use snerd_rust::task::RetryableTask;
use std::time::Duration;
use tempfile::tempdir;
use tokio::time::sleep;

#[tokio::test]
async fn test_sharded_queue_basic() {
    let dir = tempdir().unwrap();
    let q = SnerdShardedQueue::new("sharded-test", dir.path(), 4).await;
    
    // Give membership loop a moment to claim shards and start engines
    sleep(Duration::from_millis(100)).await;

    let processed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let p_clone = processed.clone();

    q.register_task_handler("my_task", move |_data| {
        p_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }).await;

    let t1 = RetryableTask::new(
        "task-1".to_string(),
        "my_task".to_string(),
        "{}".to_string(),
        1, 1.0, None, None, None, None, None, None, None, None, None, None
    );
    let t2 = RetryableTask::new(
        "task-2".to_string(),
        "my_task".to_string(),
        "{}".to_string(),
        1, 1.0, None, None, None, None, None, None, None, None, None, None
    );

    q.enqueue(t1).await.unwrap();
    q.enqueue(t2).await.unwrap();

    sleep(Duration::from_millis(1500)).await;

    assert_eq!(processed.load(std::sync::atomic::Ordering::SeqCst), 2);
}
