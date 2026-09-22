use chrono::Utc;
use fs3::FileExt;
use std::collections::{HashMap, HashSet, BinaryHeap};
use std::fs::{File, OpenOptions};
use tokio::sync::Semaphore;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use serde_json::json;

use crate::file_store::FileStore;
use crate::rate_limiter::RateLimiter;
use crate::task::{RetryableTask, PriorityTask, ProgressMessage};
use tokio::sync::broadcast;

pub type TaskHandler = Arc<dyn Fn(String) -> Result<(), String> + Send + Sync>;
pub type MaxRetryHandler = Arc<dyn Fn(String) -> Result<(), String> + Send + Sync>;

/// How long a completed task id stays in `completed_tasks` before eviction.
/// It is only a safety net against duplicate execution within a short window;
/// the tombstone in the task log is the durable source of truth.
const COMPLETED_TTL: Duration = Duration::from_secs(60);
/// How often the sweeper evicts expired completed entries.
const COMPLETED_SWEEP_INTERVAL: Duration = Duration::from_secs(30);

struct ExecutingGuard {
    executing_tasks: Arc<Mutex<HashSet<String>>>,
    task_id: String,
}

/// Drops completed entries whose completion time is older than `now - ttl`.
fn evict_expired_completed(completed: &mut HashMap<String, Instant>, now: Instant, ttl: Duration) {
    completed.retain(|_, completed_at| now.duration_since(*completed_at) < ttl);
}

impl Drop for ExecutingGuard {
    fn drop(&mut self) {
        if let Ok(mut executing) = self.executing_tasks.lock() {
            executing.remove(&self.task_id);
        }
    }
}

#[derive(Clone)]
pub struct SnerdQueue {
    pub name: String,
    pub file_store: FileStore,
    pub rate_limiter: RateLimiter,
    task_handlers: Arc<RwLock<HashMap<String, TaskHandler>>>,
    max_retry_handlers: Arc<RwLock<HashMap<String, MaxRetryHandler>>>,
    active_hashes: Arc<Mutex<HashSet<String>>>,
    executing_tasks: Arc<Mutex<HashSet<String>>>,
    /// Tasks that have been pushed to shared_pq but haven't started executing yet.
    /// Prevents process_due_tasks() from re-adding the same task to the queue.
    queued_tasks: Arc<Mutex<HashSet<String>>>,
    /// Tasks that have completed execution (successfully or max retries reached),
    /// mapped to their completion time. Final safety net to prevent duplicate
    /// execution; entries are evicted after COMPLETED_TTL to bound memory.
    completed_tasks: Arc<Mutex<HashMap<String, Instant>>>,
    worker_pools: Arc<HashMap<String, Arc<Semaphore>>>,
    pub progress_tx: broadcast::Sender<ProgressMessage>,
    /// Shared priority queues per pool
    shared_pqs: Arc<Mutex<HashMap<String, BinaryHeap<PriorityTask>>>>,
    /// Number of active dispatcher loops per pool
    dispatcher_counts: Arc<Mutex<HashMap<String, usize>>>,
    /// Exclusive OS-level lock on the task log, held for the queue's lifetime.
    /// Guarantees a single processor per storage file. Never read directly;
    /// keeping the handle alive is what keeps the lock held.
    _storage_lock: Arc<File>,
}

impl SnerdQueue {
    pub fn new(name: &str, file_store: FileStore, rate_limiter: RateLimiter) -> Self {
        let mut pools = HashMap::new();
        pools.insert("default".to_string(), 100);
        Self::new_with_pools(name, file_store, rate_limiter, pools)
    }

    pub fn new_with_pools(name: &str, file_store: FileStore, rate_limiter: RateLimiter, pools: HashMap<String, usize>) -> Self {
        // Acquire exclusive ownership of the task log before anything else.
        // Two processors on the same file would race and double-execute tasks,
        // so a second queue on the same storage fails fast instead. The OS
        // releases the lock automatically when the file is closed/exits.
        let log_path = file_store.file_path().to_path_buf();
        if let Some(parent) = log_path.parent() {
            std::fs::create_dir_all(parent).unwrap_or_else(|e| {
                panic!(
                    "[Snerd] ERROR: Could not create storage directory '{}': {}",
                    parent.display(),
                    e
                )
            });
        }
        let mut lock_path = log_path.clone().into_os_string();
        lock_path.push(".lock");
        let lock_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .unwrap_or_else(|e| {
                panic!(
                    "[Snerd] ERROR: Failed to open lock file '{}': {}",
                    std::path::PathBuf::from(&lock_path).display(),
                    e
                )
            });
        if lock_file.try_lock_exclusive().is_err() {
            panic!(
                "[Snerd] ERROR: Another queue instance is already running on storage '{}'. \
                 Use a single queue instance per storage file (register all your task types on it), \
                 or create a FileStore with a different path. (lock file: {})",
                log_path.display(),
                std::path::PathBuf::from(&lock_path).display()
            );
        }

        let mut initial_hashes = HashSet::new();
        if let Ok(tasks) = file_store.read_tasks() {
            for task in tasks {
                if task.deleted_at.is_none() {
                    if let Some(hash) = task.payload_hash {
                        initial_hashes.insert(hash);
                    }
                }
            }
        }

        let mut worker_pools = HashMap::new();
        let mut has_default = false;
        let mut shared_pqs = HashMap::new();
        let mut dispatcher_counts = HashMap::new();
        for (pool_name, capacity) in pools {
            worker_pools.insert(pool_name.clone(), Arc::new(Semaphore::new(capacity)));
            shared_pqs.insert(pool_name.clone(), BinaryHeap::new());
            dispatcher_counts.insert(pool_name.clone(), 0);
            if pool_name == "default" {
                has_default = true;
            }
        }
        if !has_default {
            worker_pools.insert("default".to_string(), Arc::new(Semaphore::new(100)));
            shared_pqs.insert("default".to_string(), BinaryHeap::new());
            dispatcher_counts.insert("default".to_string(), 0);
        }

        let (progress_tx, _) = broadcast::channel(1024);
        Self {
            name: name.to_string(),
            file_store,
            rate_limiter,
            task_handlers: Arc::new(RwLock::new(HashMap::new())),
            max_retry_handlers: Arc::new(RwLock::new(HashMap::new())),
            active_hashes: Arc::new(Mutex::new(initial_hashes)),
            executing_tasks: Arc::new(Mutex::new(HashSet::new())),
            queued_tasks: Arc::new(Mutex::new(HashSet::new())),
            completed_tasks: Arc::new(Mutex::new(HashMap::new())),
            worker_pools: Arc::new(worker_pools),
            progress_tx,
            shared_pqs: Arc::new(Mutex::new(shared_pqs)),
            dispatcher_counts: Arc::new(Mutex::new(dispatcher_counts)),
            _storage_lock: Arc::new(lock_file),
        }
    }

    pub fn subscribe_progress(&self) -> broadcast::Receiver<ProgressMessage> {
        self.progress_tx.subscribe()
    }

    pub fn yield_progress(&self, task_id: &str, data: &str) {
        let _ = self.progress_tx.send(ProgressMessage {
            task_id: task_id.to_string(),
            data: data.to_string(),
        });
    }

    pub async fn register_task_handler<F>(&self, task_type: &str, handler: F)
    where
        F: Fn(String) -> Result<(), String> + Send + Sync + 'static,
    {
        self.task_handlers
            .write()
            .await
            .insert(task_type.to_string(), Arc::new(handler));
    }

    pub async fn register_max_retry_handler<F>(&self, task_type: &str, handler: F)
    where
        F: Fn(String) -> Result<(), String> + Send + Sync + 'static,
    {
        self.max_retry_handlers
            .write()
            .await
            .insert(task_type.to_string(), Arc::new(handler));
    }

    pub fn enqueue(&self, mut task: RetryableTask) -> std::io::Result<()> {
        if let Some(ref hash) = task.payload_hash {
            if let Ok(mut hashes) = self.active_hashes.lock() {
                if hashes.contains(hash) {
                    return Ok(());
                }
                hashes.insert(hash.clone());
            }
        }
        task.deleted_at = None;
        self.file_store.save_task(&task)?;

        // NOTE: We intentionally do NOT execute tasks immediately here.
        // All execution goes through the periodic processor (process_due_tasks)
        // which uses a BinaryHeap to respect priority ordering.
        // The fast path would bypass priority and cause low-priority tasks
        // enqueued first to always execute before high-priority tasks enqueued later.

        Ok(())
    }

    pub async fn start_processor(&self, interval: Duration) {
        let q = self.clone();
        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            loop {
                interval_timer.tick().await;
                q.process_due_tasks().await;
            }
        });
        self.start_completed_sweeper();
    }

    /// Periodically evicts completed-task entries older than COMPLETED_TTL so
    /// the dedup set does not grow unboundedly on long-running processes.
    fn start_completed_sweeper(&self) {
        let completed = Arc::clone(&self.completed_tasks);
        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(COMPLETED_SWEEP_INTERVAL);
            loop {
                interval_timer.tick().await;
                evict_expired_completed(&mut completed.lock().unwrap(), Instant::now(), COMPLETED_TTL);
            }
        });
    }

    pub async fn process_due_tasks(&self) {
        let tasks = match self.file_store.read_tasks() {
            Ok(t) => t,
            Err(_) => return,
        };

        let now = Utc::now();

        // IMPORTANT: Check against LIVE executing_tasks and queued_tasks sets
        // (not snapshots) to prevent races where a task moves from queued → executing
        // between our snapshot and our check, making it invisible to both.
        let mut pools_to_dispatch = Vec::new();
        {
            let mut pqs = self.shared_pqs.lock().unwrap();
            let mut queued = self.queued_tasks.lock().unwrap();
            let executing = self.executing_tasks.lock().unwrap();
            for task in tasks {
                if task.execute_at <= now
                    && task.retry_after_time <= now
                    && task.deleted_at.is_none()
                    && !executing.contains(&task.task_id)
                    && !queued.contains(&task.task_id)
                {
                    let pool_name = task.pool.clone().unwrap_or_else(|| "default".to_string());
                    if let Some(pq) = pqs.get_mut(&pool_name) {
                        queued.insert(task.task_id.clone());
                        pq.push(PriorityTask(task));
                        pools_to_dispatch.push(pool_name);
                    } else if let Some(pq) = pqs.get_mut("default") {
                        queued.insert(task.task_id.clone());
                        pq.push(PriorityTask(task));
                        pools_to_dispatch.push("default".to_string());
                    }
                }
            }
        }

        // Start a priority dispatcher for pools with tasks and not too many dispatchers
        pools_to_dispatch.sort();
        pools_to_dispatch.dedup();
        for pool_name in pools_to_dispatch {
            let mut counts = self.dispatcher_counts.lock().unwrap();
            let count = counts.entry(pool_name.clone()).or_insert(0);
            if *count < 2 {
                *count += 1;
                self.spawn_dispatcher(pool_name);
            }
        }
    }

    /// Spawns a persistent priority dispatcher that feeds tasks to workers
    /// in strict priority order. The dispatcher acquires a semaphore permit
    /// for each task, ensuring at most 100 concurrent executions. When a task
    /// completes and releases its permit, the dispatcher wakes up and spawns
    /// the next highest-priority task.
    fn spawn_dispatcher(&self, pool_name: String) {
        let q = self.clone();
        let pool_name_clone = pool_name.clone();
        tokio::spawn(async move {
            let semaphore = match q.worker_pools.get(&pool_name_clone) {
                Some(s) => s.clone(),
                None => {
                    let mut counts = q.dispatcher_counts.lock().unwrap();
                    if let Some(c) = counts.get_mut(&pool_name_clone) { *c -= 1; }
                    return;
                }
            };
            loop {
                // Acquire a concurrency permit
                let permit = match semaphore.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => break,
                };

                // Pop the highest-priority task from the specific pool's priority queue
                let task = {
                    let mut pqs = q.shared_pqs.lock().unwrap();
                    if let Some(pq) = pqs.get_mut(&pool_name_clone) {
                        pq.pop()
                    } else {
                        None
                    }
                };

                match task {
                    Some(PriorityTask(mut task)) => {
                        // Rate limit check
                        if let Some(ref group) = task.rate_limit_group {
                            if let Some(limit) = task.max_per_minute {
                                match q.rate_limiter.check_and_increment(group, limit) {
                                    Ok(true) => {}
                                    Ok(false) | Err(_) => {
                                        task.retry_after_time = Utc::now() + chrono::Duration::seconds(60);
                                        let _ = q.file_store.save_task(&task);
                                        // Remove from queued so it can be re-queued after rate limit window
                                        q.queued_tasks.lock().unwrap().remove(&task.task_id);
                                        drop(permit);
                                        continue;
                                    }
                                }
                            }
                        }

                        // Move from queued to executing
                        {
                            let mut queued = q.queued_tasks.lock().unwrap();
                            queued.remove(&task.task_id);
                            let mut executing = q.executing_tasks.lock().unwrap();
                            if executing.contains(&task.task_id) {
                                drop(permit);
                                continue;
                            }
                            executing.insert(task.task_id.clone());
                        }

                        let q2 = q.clone();
                        tokio::spawn(async move {
                            let _permit = permit;
                            q2.execute_task(task).await;
                        });
                    }
                    None => {
                        drop(permit);
                        break; // Queue empty, dispatcher exits
                    }
                }
            }
            let mut counts = q.dispatcher_counts.lock().unwrap();
            if let Some(c) = counts.get_mut(&pool_name_clone) {
                *c -= 1;
            }
        });
    }

    async fn execute_task(&self, mut task: RetryableTask) {
        // Final safety check: skip if already completed (prevents duplicate execution)
        {
            let completed = self.completed_tasks.lock().unwrap();
            if completed.contains_key(&task.task_id) {
                return; // Already completed, skip
            }
        }

        // Drop guard guarantees removal from executing_tasks
        let _guard = ExecutingGuard {
            executing_tasks: Arc::clone(&self.executing_tasks),
            task_id: task.task_id.clone(),
        };

        // Build the execution result — either via webhook HTTP call or local handler
        let result: Result<(), String> = if let Some(ref url) = task.webhook_url.clone() {
            // --- Webhook Path ---
            let payload = json!({
                "taskId": task.task_id,
                "taskType": task.task_type,
                "data": task.task_data,
            });
            let url = url.clone();
            tokio::task::spawn_blocking(move || {
                let rt = tokio::runtime::Handle::current();
                rt.block_on(async {
                    let mut client_builder = reqwest::Client::builder();
                    if let Some(secs) = task.max_execution_seconds {
                        client_builder = client_builder.timeout(std::time::Duration::from_secs(secs));
                    }
                    let client = client_builder.build().unwrap_or_else(|_| reqwest::Client::new());
                    
                    match client
                        .post(&url)
                        .header("Content-Type", "application/json")
                        .header("X-SnerdMQ-Event", "Execute")
                        .json(&payload)
                        .send()
                        .await
                    {
                        Ok(resp) if resp.status().is_success() => Ok(()),
                        Ok(resp) => Err(format!("Webhook returned non-2xx status: {}", resp.status())),
                        Err(e) => {
                            if e.is_timeout() {
                                Err(format!("Webhook execution timed out after {} seconds", task.max_execution_seconds.unwrap_or(0)))
                            } else {
                                Err(format!("Webhook request failed: {}", e))
                            }
                        }
                    }
                })
            })
            .await
            .unwrap_or_else(|e| Err(format!("Webhook task panic: {:?}", e)))
        } else {
            // --- Local Handler Path ---
            let handler = {
                let handlers = self.task_handlers.read().await;
                handlers.get(&task.task_type).cloned()
            };
            if let Some(h) = handler {
                let task_data = task.task_data.clone();
                let fut = tokio::task::spawn_blocking(move || h(task_data));
                
                if let Some(secs) = task.max_execution_seconds {
                    match tokio::time::timeout(std::time::Duration::from_secs(secs), fut).await {
                        Ok(Ok(res)) => res,
                        Ok(Err(e)) => Err(format!("Task panic: {:?}", e)),
                        Err(_) => Err(format!("Task execution timed out after {} seconds", secs)),
                    }
                } else {
                    fut.await.unwrap_or_else(|e| Err(format!("Task panic: {:?}", e)))
                }
            } else {
                return; // No handler and no webhook — nothing to do
            }
        };

        match result {
            Ok(_) => {
                let mut rescheduled = false;
                if let Some(ref cron_expr) = task.cron_expression {
                    use cron::Schedule;
                    use std::str::FromStr;
                    if let Ok(schedule) = Schedule::from_str(cron_expr) {
                        if let Some(next) = schedule.upcoming(Utc).next() {
                            task.execute_at = next;
                            task.retry_count = 0;
                            task.last_error_obj = None;
                            task.last_job_error = None;
                            let _ = self.file_store.save_task(&task);
                            rescheduled = true;
                        }
                    }
                }

                if !rescheduled {
                    // Mark as completed to prevent duplicate execution
                    self.completed_tasks.lock().unwrap().insert(task.task_id.clone(), Instant::now());
                    let _ = self.file_store.delete_task(&task.task_id);
                    if let Some(ref hash) = task.payload_hash {
                        if let Ok(mut hashes) = self.active_hashes.lock() {
                            hashes.remove(hash);
                        }
                    }
                }
            }
            Err(e) => {
                // max_retries means total attempts (not retries after first).
                // retry_count starts at 0 and update_retry_config increments it AFTER this check.
                // So we allow retry while retry_count < max_retries - 1.
                if task.retry_count < task.max_retries - 1 {
                    task.update_retry_config(Some(e));
                    let _ = self.file_store.save_task(&task);
                } else {
                    // Max retries reached — fire DLQ webhook or local max retry handler
                    if let Some(ref url) = task.webhook_url.clone() {
                        let payload = json!({
                            "taskId": task.task_id,
                            "taskType": task.task_type,
                            "data": task.task_data,
                        });
                        let url = url.clone();
                        tokio::spawn(async move {
                            let _ = reqwest::Client::new()
                                .post(&url)
                                .header("Content-Type", "application/json")
                                .header("X-SnerdMQ-Event", "MaxRetriesReached")
                                .json(&payload)
                                .send()
                                .await;
                        });
                    } else {
                        let max_handler = {
                            let max_handlers = self.max_retry_handlers.read().await;
                            max_handlers.get(&task.task_type).cloned()
                        };
                        if let Some(mh) = max_handler {
                            // Pass full task info as JSON to DLQ handler
                            let dlq_payload = serde_json::to_string(&json!({
                                "taskId": task.task_id,
                                "taskType": task.task_type,
                                "data": task.task_data,
                            })).unwrap_or_else(|_| task.task_data.clone());
                            let _ = tokio::task::spawn_blocking(move || mh(dlq_payload)).await;
                        }
                    }

                    // Mark as completed to prevent duplicate execution
                    self.completed_tasks.lock().unwrap().insert(task.task_id.clone(), Instant::now());
                    let _ = self.file_store.delete_task(&task.task_id);
                    if let Some(ref hash) = task.payload_hash {
                        if let Ok(mut hashes) = self.active_hashes.lock() {
                            hashes.remove(hash);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod completed_eviction_tests {
    use super::*;

    #[test]
    fn evicts_entries_older_than_ttl() {
        let now = Instant::now();
        let mut completed = HashMap::new();
        completed.insert("stale".to_string(), now - Duration::from_secs(90));
        completed.insert("fresh".to_string(), now - Duration::from_secs(10));

        evict_expired_completed(&mut completed, now, COMPLETED_TTL);

        assert!(!completed.contains_key("stale"));
        assert!(completed.contains_key("fresh"));
        assert_eq!(completed.len(), 1);
    }

    #[test]
    fn keeps_entries_exactly_within_ttl() {
        let now = Instant::now();
        let mut completed = HashMap::new();
        completed.insert("edge".to_string(), now - (COMPLETED_TTL - Duration::from_secs(1)));

        evict_expired_completed(&mut completed, now, COMPLETED_TTL);

        assert!(completed.contains_key("edge"));
    }

    #[test]
    fn empty_map_is_noop() {
        let mut completed: HashMap<String, Instant> = HashMap::new();
        evict_expired_completed(&mut completed, Instant::now(), COMPLETED_TTL);
        assert!(completed.is_empty());
    }
}
