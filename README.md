<div align="center">
  <img src="./assets/Designer-9.png" height="120" alt="Snerd-Rust Logo" />
  <h1>⚙️ snerd-rust v0.3.0</h1>
  <p>A blazingly fast, brutally simple, zero-infrastructure async background job engine for Rust.</p>

  [![Crates.io](https://img.shields.io/crates/v/snerd-rust.svg)](https://crates.io/crates/snerd-rust)
  [![Documentation](https://docs.rs/snerd-rust/badge.svg)](https://docs.rs/snerd-rust)
  [![CI](https://github.com/speed-nerd/snerd-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/speed-nerd/snerd-rust/actions/workflows/ci.yml)
  [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
  [![Docs](https://img.shields.io/badge/docs-speed--nerd.github.io-blue)](https://speed-nerd.github.io/docs/)
</div>

If you are tired of wrestling with heavy, bloated background job frameworks like Redis, Postgres tables, or RabbitMQ just to send a few emails in the background... well, you are in the right place.

`snerd-rust` is an embedded, high-performance background task queue that lives entirely in a single, perfectly OS-locked, append-only `.log` file on your file system. It was designed to bring the aggressive concurrency and lightweight footprint of Golang's `snerd` over to Rust's heavily optimized asynchronous ecosystem.

No databases. No external daemons. No nonsense.

---

## 🔥 Features

* **Zero External Infrastructure**: You don't need a Redis cluster. Your tasks are persisted directly to `.snerdata/tasks/tasks.log` using standard filesystem I/O.
* **Built-in Web Dashboard**: A one-line `queue.start_dashboard(port)` serves a live React UI with queue stats, job table, and a real-time progress stream.
* **Bulletproof File Locks**: Safely scales across multiple processes! We utilize OS-level file-locking boundaries (`flock`) to guarantee that your tasks are never corrupted, even if multiple instances of your app try to write simultaneously.
* **Smart API Rate-Limiting**: Natively tracks `rate_limit_group` execution velocity to prevent 429 "Too Many Requests" API errors.
* **Payload-Hashing Deduplication**: Automatically computes cryptographic hashes to drop duplicate tasks instantly.
* **Dynamic Float Prioritization**: A native Binary Max-Heap bypasses standard FIFO rules for high urgency tasks.
* **Cron, Webhooks & Hard Timeouts**: Recurring schedules, serverless HTTP execution, and per-task execution timeouts.
* **Progress Streaming**: Handlers can emit live progress events that stream straight into the dashboard.
* **Asynchronous Tokio Core**: Built natively on top of `tokio`. Background workers process the queue without starving your main event loop.
* **Dead-Letter Queue (DLQ)**: Built-in `max_retries` limits and hooks to elegantly catch and bury poison-pill tasks.
* **Sharded Queues**: Distribute load across multiple queue nodes safely using file-backed lock sharding.
* **Worker Pools**: Prevent slow tasks from starving fast tasks by dedicating workers to specific pools.
* **Job Chaining**: Sequence tasks as DAGs using `trigger_after_ids`.

---

## 📦 Installation

Just add `snerd-rust` to your `Cargo.toml`:

```toml
[dependencies]
snerd-rust = "0.3.0"
tokio = { version = "1", features = ["full"] }
```

*Note: `snerd-rust` is entirely async, so you need a `tokio` runtime to drive it.*

---

## 🚀 Quickstart (Basic)

It takes roughly 3 lines of code to spin up a queue and start firing background jobs.

```rust
use snerd_rust::file_store::FileStore;
use snerd_rust::queue::SnerdQueue;
use snerd_rust::rate_limiter::RateLimiter;
use snerd_rust::task::RetryableTask;
use std::time::Duration;

#[tokio::main]
async fn main() {
    // 1. Initialize the Persistence Store
    let file_store = FileStore::new(".snerdata/tasks/tasks.log").unwrap();

    // 2. Create the Queue (name, persistence store, rate limiter)
    let queue = SnerdQueue::new(
        "my-fast-queue",
        file_store,
        RateLimiter::new(&std::path::PathBuf::from(".snerdata")),
    );

    // 3. Register your Task Handler (the closure that does the actual work)
    queue.register_task_handler("generate_ai_image", |data| {
        println!("Generating with payload: {}", data);
        // ... do your heavy lifting here!
        Ok(()) // Return Err("...".to_string()) to trigger a retry!
    }).await;

    // 4. (Optional) Register a Dead-Letter Handler for when retries run out
    queue.register_max_retry_handler("generate_ai_image", |data| {
        println!("Task permanently failed! Payload: {}", data);
        Ok(())
    }).await;

    // 5. Boot the background processor polling loop
    queue.start_processor(Duration::from_secs(2)).await;

    // 6. Enqueue a task! (max_retries=3, retry_after_hours=1.0)
    let task = RetryableTask::new(
        "unique-task-id-123".to_string(), // ID
        "generate_ai_image".to_string(),  // Type (matches handler)
        r#"{"prompt": "A crab in space"}"#.to_string(), // JSON payload
        3,    // Max retries
        1.0,  // Delay in hours before a failed task is retried
        None, None, None, None,           // rate group, max/min, dedupe, urgency
        None,                             // execute_at
        None,                             // cron
        None,                             // webhook_url
        None,                             // max_execution_seconds
        None,                             // pool
        None,                             // trigger_after_ids
    );
    queue.enqueue(task).unwrap();

    // Keep your app alive — jobs run on background tokio tasks
    tokio::time::sleep(Duration::from_secs(10)).await;
}
```

---

## ⚙️ Advanced Task Configuration

To power complex workflows, `RetryableTask::new` accepts advanced orchestration parameters (pass `None` for anything you don't need):

```rust
let task = RetryableTask::new(
    "unique-task-id-123".to_string(),
    "generate_ai_image".to_string(),
    r#"{"prompt": "A crab in space"}"#.to_string(),
    3,                                        // max_retries
    1.0,                                      // retry_after_hours
    Some("openai_api".to_string()),           // rate_limit_group
    Some(50),                                 // max_per_minute
    Some(true),                               // auto_dedupe
    Some(0.95),                               // urgency_score
    None,                                     // execute_at (RFC3339 string)
    Some("1h".to_string()),                   // cron — runs every 1 hour
    None,                                     // webhook_url
    Some(300),                                // max_execution_seconds
    Some("urgent".to_string()),               // pool
    Some(vec!["parent-job-123".to_string()]), // trigger_after_ids
);
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `max_retries` | `i32` | — | How many times a failed task is retried before hitting the Dead Letter Queue. |
| `retry_after_hours` | `f64` | — | Backoff in **hours** before a failed task is retried (e.g. `0.001` ≈ seconds). |
| `auto_dedupe` | `Option<bool>` | `None` | If `true`, a cryptographic hash of `task_type` + `task_data` is computed. If an identical payload is already pending, the new task is silently dropped. Excellent for preventing duplicate generative AI requests from trigger-happy users! |
| `urgency_score` | `Option<f64>` | `None` | A value (e.g. `0.99`) used to bypass the standard FIFO queue. A Binary Max-Heap continually floats the highest urgency tasks to the front of the execution line. Standard tasks default to `0.0`. |
| `rate_limit_group` | `Option<String>` | `None` | A custom string (e.g. `"openai_api"` or `"db_writes"`) that groups tasks together for backpressure control. |
| `max_per_minute` | `Option<i32>` | `None` | Used with `rate_limit_group`. If the group exceeds this limit in a 60-second rolling window, further tasks in the group pause for a minute — natively preventing 429 errors. |
| `execute_at` | `Option<String>` | `None` | An RFC3339 timestamp of when the job should first run (delayed execution). |
| `cron` | `Option<String>` | `None` | A cron expression for recurring jobs: standard 5-field (`"0 * * * *"`), 6-field with seconds (`"*/10 * * * * *"`), or shorthands `"30s"`, `"10m"`, `"2h"`, `"1d"`. |
| `webhook_url` | `Option<String>` | `None` | Optional webhook URL — the payload is dispatched via HTTP POST instead of a local handler. |
| `max_execution_seconds` | `Option<u64>` | `None` | Optional hard timeout in seconds (see below). |
| `pool` | `Option<String>` | `None` | Dedicate this task to a specific worker pool (e.g. `"urgent"`). Use `SnerdQueue::new_with_pools` to allocate workers per pool. |
| `trigger_after_ids` | `Option<Vec<String>>` | `None` | Wait for other tasks (by `task_id`) to successfully complete before executing this task. |

### ⏱️ Note on Hard Timeouts (`max_execution_seconds`)
When `max_execution_seconds` is provided, the engine wraps the execution in a `tokio::time::timeout`. If the task takes longer than the timeout, the engine cancels the task, frees up the worker slot, and marks the execution as failed (it will be retried if `max_retries` allows).

### 🌐 HTTP Webhooks (Serverless Execution)
You can configure a task to execute externally via an HTTP POST request. By setting a `webhook_url`, the background processor skips any registered handlers and directly invokes the HTTP endpoint with the payload and the header `X-SnerdMQ-Event: Execute`.

If the endpoint returns a non-2xx status code, it triggers a retry. If it permanently fails (reaches `max_retries`), the Dead Letter Queue event is automatically fired via a final HTTP POST to the same `webhook_url` with the header `X-SnerdMQ-Event: MaxRetriesReached`.

### 🕒 Cron Jobs vs. Retryable Jobs
When using the scheduling features, it is important to understand the difference between Cron and Retry behaviors:
> - **A Cron Job** is a *Repeatable Job* that executes again **only after a success**, on a fixed schedule.
> - **A Retryable Job** is a *Recovery Job* that executes again **only after a failure**, attempting to recover using the `retry_after_hours` backoff.
> - **Combined:** If a Cron Job fails, it temporarily uses `retry_after_hours` to retry until it recovers. Once it succeeds, it goes back to ticking on its standard cron schedule!

### ☠️ Dead Letter Queue (Handling Permanent Failures)
The DLQ captures tasks that have exhausted all `max_retries`. Define a custom handler with `queue.register_max_retry_handler(task_type, handler)` — critical for alerting or manual intervention when a background process consistently fails.

---

## 📊 Live Dashboard

`snerd-rust` ships with a built-in **React UI dashboard** served directly by the library — no extra services or dependencies required. It gives you a real-time window into your queue:

- **Live stats**: total enqueued, processed, and failed jobs
- **Recent Jobs table**: per-task status (`queued`, `active`, `completed`, `failed`, `dead_letter`), retry counts, and badges showing which features a task uses (cron / webhook / timeout)
- **Real-time Progress Stream**: live output from `yield_progress` calls in your handlers

```rust
// Start the built-in dashboard on http://localhost:9090
queue.start_dashboard(9090);
```

Then open **http://localhost:9090** in your browser. The page polls a small JSON API exposed by the library — also handy if you want to build your own tooling on top:

| Endpoint | Returns |
|---|---|
| `/api/stats` | `{"enqueued": N, "processed": N, "failed": N}` |
| `/api/tasks` | All jobs with status, retries, cron, webhook, timeout info |
| `/api/progress` | The last 100 progress events (`{ts, task_id, data}`) |

**Serving the UI:** the dashboard page is the single file `static/index.html`, resolved relative to your process's working directory. The bundle ships with this repo under `static/` — run your binary from the directory that contains the `static/` folder (or copy the folder next to your binary).

> **Note:** `start_dashboard` only serves the UI — your jobs keep running whether or not the dashboard is open.

---

## 📡 Progress Reporting

Long-running handlers can stream live updates to the Dashboard's Progress Stream (ideal for streaming LLM tokens or multi-step ETL work). Clone the queue into your handler and call `yield_progress`:

```rust
let q = queue.clone();
queue.register_task_handler("generate_report", move |data| {
    for step in 1..=10 {
        do_work(step);
        q.yield_progress("report-task-1", &format!("Step {}/10 complete", step));
    }
    Ok(())
}).await;
```

You can also subscribe to the raw progress feed from your own code (`tokio::sync::broadcast` receiver of `ProgressMessage { task_id, data }`):

```rust
let mut rx = queue.subscribe_progress();
tokio::spawn(async move {
    while let Ok(msg) = rx.recv().await {
        println!("progress for {}: {}", msg.task_id, msg.data);
    }
});
```

---

## 🧩 Queue Topology: One Queue or Many?

### ✅ Recommended: one queue, all job types (singleton)

The recommended pattern is **one queue instance per application**: register every job type on it and serve a single shared dashboard:

```rust
use snerd_rust::file_store::FileStore;
use snerd_rust::queue::SnerdQueue;
use snerd_rust::rate_limiter::RateLimiter;
use snerd_rust::task::RetryableTask;
use std::time::Duration;

#[tokio::main]
async fn main() {
    // ONE queue for the whole app
    let file_store = FileStore::new(".snerdata/tasks/tasks.log").unwrap();
    let queue = SnerdQueue::new(
        "main",
        file_store,
        RateLimiter::new(&std::path::PathBuf::from(".snerdata")),
    );

    // Job type #1: image processing
    queue.register_task_handler("process_image", |data| {
        println!("Processing image: {}", data);
        Ok(())
    }).await;

    // Job type #2: OTP emails — same queue
    queue.register_task_handler("send_otp_email", |data| {
        println!("Sending OTP: {}", data);
        Ok(())
    }).await;

    queue.start_processor(Duration::from_secs(2)).await;

    // Both job types flow through the exact same queue
    queue.enqueue(RetryableTask::new(
        "img-1".to_string(), "process_image".to_string(),
        r#"{"image_id": "abc123"}"#.to_string(),
        3, 0.5, None, None, None, None, None, None, None, None, None, None,
    )).unwrap();

    queue.enqueue(RetryableTask::new(
        "otp-1".to_string(), "send_otp_email".to_string(),
        r#"{"to": "john@wick.com"}"#.to_string(),
        3, 0.5, None, None, None, None, None, None, None, None, None, None,
    )).unwrap();

    // ONE dashboard shows every job type
    queue.start_dashboard(9090);

    tokio::time::sleep(Duration::from_secs(10)).await;
}
```

All job types share everything: the same persistent job log, retry/DLQ pipeline, rate-limit state, stats — and one dashboard at `http://localhost:9090` showing all of them.

### 🚫 Same storage twice = fails fast

`SnerdQueue::new` takes an **exclusive OS-level lock** on the storage file. A second queue on the same storage fails instead of silently double-executing your tasks:

```rust
let store1 = FileStore::new(".snerdata/tasks/tasks.log").unwrap();
let first = SnerdQueue::new("main", store1, RateLimiter::new(&std::path::PathBuf::from(".snerdata"))); // ✅

let store2 = FileStore::new(".snerdata/tasks/tasks.log").unwrap();
let second = SnerdQueue::new("other", store2, RateLimiter::new(&std::path::PathBuf::from(".snerdata"))); // ❌ panics:
// "[Snerd] ERROR: Another queue instance is already running on storage ..."
```

This applies across processes too — a second process pointed at the same log file also fails to start its queue.

### 🔀 Need multiple queues? Give each one its own storage

```rust
let images = SnerdQueue::new(
    "images",
    FileStore::new(".snerdata-images/tasks.log").unwrap(),
    RateLimiter::new(&std::path::PathBuf::from(".snerdata-images")),
);
let emails = SnerdQueue::new(
    "emails",
    FileStore::new(".snerdata-emails/tasks.log").unwrap(),
    RateLimiter::new(&std::path::PathBuf::from(".snerdata-emails")),
);

images.start_dashboard(9090); // separate dashboards, so separate ports
emails.start_dashboard(9091);
```

Now you have two fully independent engines: separate job logs, separate rate-limit state, separate dashboards. Only split when you actually need isolation (different cadence, different retention, independent monitoring) — otherwise the singleton is simpler and recommended.

---

## 🌍 Advanced: Distributed Scaling

A queue instance exclusively owns its storage file: `SnerdQueue::new` takes an OS-level lock (`<tasks.log>.lock`) and holds it for the queue's lifetime. A second queue pointed at the same file — in the same process or on another server — panics instead of racing it and double-executing tasks.

Scaling out therefore means **one queue per server**, each with its own `FileStore`. Your load balancer routes requests across servers, and every server processes the tasks it enqueued:

```rust
// Each server runs its own queue on its own log file (local disk works fine)
let file_store = FileStore::new("/var/data/snerd/tasks.log").unwrap();
```

A shared network drive (AWS EFS or NFS) is still a good home for that log when a single instance needs durable storage — e.g. a container that restarts but must keep its queue state. OS-level file locking keeps writes safe.

---

## 🔧 Queue API Reference

| API | Description |
|---|---|
| `SnerdQueue::new(name, file_store, rate_limiter)` | Create a queue from a persistence store and rate limiter. Cheap to `.clone()` (shared state). Panics if another queue instance already owns the storage file. |
| `queue.enqueue(task)` | Enqueue a task. Due tasks execute immediately on background tokio tasks; the rest are picked up by the processor loop. |
| `queue.register_task_handler(type, handler)` | Register `Fn(String) -> Result<(), String>` for a task type (runs on a blocking worker). |
| `queue.register_max_retry_handler(type, handler)` | Register the Dead-Letter handler for a task type. |
| `queue.start_processor(interval)` | Boot the background polling loop that executes due tasks. |
| `queue.process_due_tasks()` | Manually trigger one processing sweep. |
| `queue.start_dashboard(port)` | Serve the built-in dashboard UI on the given port. |
| `queue.yield_progress(task_id, data)` | Emit a progress event (dashboard Progress Stream / `subscribe_progress`). |
| `queue.subscribe_progress()` | Get a `broadcast::Receiver<ProgressMessage>` of live progress events. |
| `FileStore::read_tasks()` / `get_latest_task(id)` / `delete_task(id)` / `compact_log()` | Inspect, delete, and compact the persisted task log. |

---

## 🧠 Architecture Details

`snerd-rust` utilizes an **Append-Only Log Model** to achieve massive write speeds.
Instead of updating rows in a database, every time a task is enqueued, updated, or deleted, a brand new JSON line is instantly appended to the end of the log file.

When the `SnerdQueue` wakes up on its polling interval, it scans the log, maps out the absolute latest state of every task, and spawns parallel tokio tasks for anything that is currently due (`execute_at <= now` and `retry_after_time <= now`). Up to 100 tasks execute concurrently via an internal worker semaphore.

If your file ever grows too large (default `20MB` or >10k operations), `snerd-rust` atomically clones, shrinks, and replaces the file in the background (Log Compaction) to keep disk space minimal.

---

## 🤝 License

MIT License. Do whatever you want with it, just don't let your tasks die unhandled.
