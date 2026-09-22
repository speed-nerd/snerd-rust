use std::time::Duration;

use snerd_rust::file_store::FileStore;
use snerd_rust::queue::SnerdQueue;
use snerd_rust::rate_limiter::RateLimiter;
use snerd_rust::task::RetryableTask;

#[tokio::main]
async fn main() {
    let store = FileStore::new(".snerdata_dashboard_demo/tasks/tasks.log").unwrap();
    let limiter = RateLimiter::new(&std::path::PathBuf::from("."));
    let queue = SnerdQueue::new("dashboard-demo", store, limiter);

    // A handler that succeeds and streams progress
    let q_progress = queue.clone();
    queue
        .register_task_handler("progress-task", move |data| {
            for i in 1..=5 {
                q_progress.yield_progress(
                    "progress-task-1",
                    &format!("step {}/5 of {}", i, data),
                );
                std::thread::sleep(Duration::from_millis(300));
            }
            Ok(())
        })
        .await;

    // A handler that always fails (exercises retry/dead-letter path)
    queue
        .register_task_handler("fail-task", |_data| Err("boom: intentional failure".to_string()))
        .await;

    // Cron ping handler — emits a progress event on every cron fire
    let q_cron = queue.clone();
    queue
        .register_task_handler("cron-ping", move |_data| {
            q_cron.yield_progress(
                "cron-ping-1",
                &format!("cron ping at {}", chrono::Utc::now().to_rfc3339()),
            );
            Ok(())
        })
        .await;

    // Slow handler — sleeps 6s so the 2s hard timeout trips
    queue
        .register_task_handler("slow-task", |_data| {
            std::thread::sleep(Duration::from_secs(6));
            Ok(())
        })
        .await;

    queue.start_processor(Duration::from_secs(1)).await;
    queue.start_dashboard(9021);

    // 1) Success task with live progress
    queue
        .enqueue(RetryableTask::new(
            "progress-task-1".to_string(),
            "progress-task".to_string(),
            r#"{"job":"demo"}"#.to_string(),
            2, 0.0, None, None, None, None, None, None, None, None, None,
        ))
        .unwrap();

    // 2) Failing task with tiny max retries → dead letter
    queue
        .enqueue(RetryableTask::new(
            "fail-task-1".to_string(),
            "fail-task".to_string(),
            "{}".to_string(),
            1, 0.0001, None, None, None, None, None, None, None, None, None,
        ))
        .unwrap();

    // 3) Future-scheduled task → stays "queued"
    let future = (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
    queue
        .enqueue(RetryableTask::new(
            "future-task-1".to_string(),
            "progress-task".to_string(),
            "{}".to_string(),
            1, 0.0, None, None, None, None, Some(future), None, None, None, None,
        ))
        .unwrap();

    // 4) Cron job — refires every 10 seconds
    queue
        .enqueue(RetryableTask::new(
            "cron-ping-1".to_string(),
            "cron-ping".to_string(),
            "{}".to_string(),
            2, 0.0, None, None, None, None, None,
            Some("*/10 * * * * *".to_string()),
            None, None, None,
        ))
        .unwrap();

    // 5) Webhook job — executed via HTTP POST to the mock webhook server
    queue
        .enqueue(RetryableTask::new(
            "webhook-task-1".to_string(),
            "webhook-task".to_string(),
            r#"{"via":"webhook"}"#.to_string(),
            2, 0.0, None, None, None, None, None, None,
            Some("http://localhost:9010/webhook-ok".to_string()),
            None,
            None,
        ))
        .unwrap();

    // 6) Hard-timeout job — handler sleeps 6s but the timeout is 2s
    queue
        .enqueue(RetryableTask::new(
            "timeout-task-1".to_string(),
            "slow-task".to_string(),
            "{}".to_string(),
            1, 0.0005, None, None, None, None, None, None, None,
            Some(2),
            None,
        ))
        .unwrap();

    println!("Demo running — dashboard on http://localhost:9021 (Ctrl+C to stop)");

    // Emit a heartbeat progress event every 3 seconds so the Progress Stream stays live
    let mut tick = 0u64;
    loop {
        tokio::time::sleep(Duration::from_secs(3)).await;
        tick += 1;
        queue.yield_progress(
            "heartbeat",
            &format!("tick #{} — queue healthy at {}", tick, chrono::Utc::now().to_rfc3339()),
        );
    }
}
