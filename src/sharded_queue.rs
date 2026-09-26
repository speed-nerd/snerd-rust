use crate::file_store::FileStore;
use crate::membership::{self, MembershipStore, owner_id, skew_margin};
use crate::queue::{SnerdQueue, TaskHandler, MaxRetryHandler};
use crate::rate_limiter::RateLimiter;
use crate::sharding::{self, route_shard};
use crate::task::{ProgressMessage, RetryableTask};
use chrono::Utc;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tokio::time::{sleep, Duration};
use uuid::Uuid;

#[derive(Clone)]
pub struct SnerdShardedQueue {
    pub name: String,
    pub dir: PathBuf,
    shards: Arc<RwLock<HashMap<String, SnerdQueue>>>,
    pub progress_tx: broadcast::Sender<ProgressMessage>,
    task_handlers: Arc<RwLock<HashMap<String, TaskHandler>>>,
    max_retry_handlers: Arc<RwLock<HashMap<String, MaxRetryHandler>>>,
    pub stop_flag: Arc<std::sync::atomic::AtomicBool>,
    worker_pools: Arc<HashMap<String, Arc<tokio::sync::Semaphore>>>,
    shared_pqs: Arc<std::sync::Mutex<HashMap<String, std::collections::BinaryHeap<crate::task::PriorityTask>>>>,
    dispatcher_counts: Arc<std::sync::Mutex<HashMap<String, usize>>>,
}

impl SnerdShardedQueue {
    pub async fn new(name: &str, dir_path: &Path, requested_shards: u32) -> Self {
        let dir = dir_path.to_path_buf();
        std::fs::create_dir_all(&dir).unwrap();

        // 1. Resolve layout (migrates legacy tasks if necessary)
        let total_shards = sharding::resolve_layout(&dir, name, requested_shards)
            .unwrap_or_else(|e| panic!("Failed to resolve sharding layout: {}", e));

        let (progress_tx, _) = broadcast::channel(1024);
        
        let mut worker_pools = HashMap::new();
        worker_pools.insert("default".to_string(), Arc::new(tokio::sync::Semaphore::new(100)));
        let mut shared_pqs = HashMap::new();
        shared_pqs.insert("default".to_string(), std::collections::BinaryHeap::new());
        let mut dispatcher_counts = HashMap::new();
        dispatcher_counts.insert("default".to_string(), 0);

        let sq = Self {
            name: name.to_string(),
            dir: dir.clone(),
            shards: Arc::new(RwLock::new(HashMap::new())),
            progress_tx,
            task_handlers: Arc::new(RwLock::new(HashMap::new())),
            max_retry_handlers: Arc::new(RwLock::new(HashMap::new())),
            stop_flag: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            worker_pools: Arc::new(worker_pools),
            shared_pqs: Arc::new(std::sync::Mutex::new(shared_pqs)),
            dispatcher_counts: Arc::new(std::sync::Mutex::new(dispatcher_counts)),
        };

        sq.start_membership_heartbeat(total_shards).await;
        sq
    }

    pub fn subscribe_progress(&self) -> broadcast::Receiver<ProgressMessage> {
        self.progress_tx.subscribe()
    }

    pub async fn register_task_handler<F>(&self, task_type: &str, handler: F)
    where
        F: Fn(String) -> Result<(), String> + Send + Sync + 'static,
    {
        let h = Arc::new(handler);
        self.task_handlers.write().await.insert(task_type.to_string(), h.clone());
        let shards = self.shards.read().await;
        for (_, queue) in shards.iter() {
            let h_clone = h.clone();
            queue.register_task_handler(task_type, move |data| h_clone(data)).await;
        }
    }

    pub async fn register_max_retry_handler<F>(&self, task_type: &str, handler: F)
    where
        F: Fn(String) -> Result<(), String> + Send + Sync + 'static,
    {
        let h = Arc::new(handler);
        self.max_retry_handlers.write().await.insert(task_type.to_string(), h.clone());
        let shards = self.shards.read().await;
        for (_, queue) in shards.iter() {
            let h_clone = h.clone();
            queue.register_max_retry_handler(task_type, move |data| h_clone(data)).await;
        }
    }

    pub async fn enqueue(&self, mut task: RetryableTask) -> Result<(), String> {
        let owned_shards: Vec<String> = {
            let s = self.shards.read().await;
            s.keys().cloned().collect()
        };

        if owned_shards.is_empty() {
            return Err("Engine is in standby mode: zero shards owned. Cannot enqueue.".to_string());
        }

        if task.task_id.is_empty() {
            task.task_id = Uuid::new_v4().to_string();
        }

        let target_shard = route_shard(&task.task_id, &owned_shards).to_string();
        
        let shards = self.shards.read().await;
        if let Some(queue) = shards.get(&target_shard) {
            queue.enqueue(task).map_err(|e| e.to_string())
        } else {
            Err("Target shard not found".to_string())
        }
    }

    pub async fn get_shards(&self) -> tokio::sync::RwLockReadGuard<'_, HashMap<String, SnerdQueue>> {
        self.shards.read().await
    }

    async fn start_membership_heartbeat(&self, total_shards: u32) {
        let dir = self.dir.clone();
        let owner = owner_id();
        let sq = self.clone();

        tokio::spawn(async move {
            let store = MembershipStore::new(&dir);
            loop {
                if sq.stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                let now = Utc::now();
                let skew = skew_margin();
                let mut current_owned = Vec::new();
                
                // Read membership to see claimable shards
                if let Ok(m) = store.load() {
                    let claimable = membership::claimable_shards(&m, now, skew, &owner);
                    for shard in claimable {
                        match store.claim(&shard, &owner, now, skew) {
                            Ok(outcome) => {
                                // Try to lock the shard's .lock file
                                if let Ok(Some(_lock)) = sharding::try_lock_shard(&dir, &shard) {
                                    current_owned.push(shard.clone());
                                    
                                    // If we just claimed it, instantiate the internal queue
                                    if outcome == membership::ClaimOutcome::Claimed || matches!(outcome, membership::ClaimOutcome::TakenOver { .. }) {
                                        sq.instantiate_shard(&shard, _lock).await;
                                    }
                                } else {
                                    // Could not lock OS flock, revert membership claim
                                    let _ = store.revert_claim(&shard, &owner);
                                }
                            }
                            Err(_) => {}
                        }
                    }
                }
                
                // Cleanup lost shards
                sq.reconcile_shards(&current_owned).await;

                sleep(Duration::from_secs(membership::RENEW_INTERVAL_SECS)).await;
            }
        });
    }

    async fn instantiate_shard(&self, shard: &str, _lock: sharding::ShardLock) {
        let mut shards = self.shards.write().await;
        if shards.contains_key(shard) {
            return; // already instantiated
        }
        
        let shard_dir = sharding::shard_dir(&self.dir, shard);
        let log_path = shard_dir.join(sharding::LEGACY_TASKS_DIR).join(sharding::LEGACY_TASKS_LOG);
        
        // Ensure dir exists
        std::fs::create_dir_all(log_path.parent().unwrap()).unwrap();
        
        let file_store = FileStore::new(log_path.to_str().unwrap()).unwrap();
        let rate_limiter = RateLimiter::new(&shard_dir); // Use shard_dir for rate limits so they are partitioned

        // We already hold the `.lock` via ShardLock, but `SnerdQueue::new` will also try to take it.
        // Wait, `SnerdQueue::new` takes `.lock`. Since it's in the same process, OS flock on Unix
        // *might* allow re-entrant locks or fail. Actually, we should just let `SnerdQueue` acquire it,
        // and we drop our `_lock` right before creating `SnerdQueue`.
        drop(_lock);

        let queue = SnerdQueue::new_with_shared_pools(
            &self.name,
            file_store,
            rate_limiter,
            Arc::clone(&self.worker_pools),
            Arc::clone(&self.shared_pqs),
            Arc::clone(&self.dispatcher_counts),
        );
        
        // Wire up progress events
        let mut rx = queue.subscribe_progress();
        let tx = self.progress_tx.clone();
        tokio::spawn(async move {
            while let Ok(msg) = rx.recv().await {
                let _ = tx.send(msg);
            }
        });

        // Copy over task handlers
        {
            let handlers = self.task_handlers.read().await;
            for (k, v) in handlers.iter() {
                let v_clone = v.clone();
                queue.register_task_handler(k, move |data| v_clone(data)).await;
            }
        }
        {
            let max_handlers = self.max_retry_handlers.read().await;
            for (k, v) in max_handlers.iter() {
                let v_clone = v.clone();
                queue.register_max_retry_handler(k, move |data| v_clone(data)).await;
            }
        }

        // Start processor
        queue.start_processor(std::time::Duration::from_secs(1)).await;
        
        shards.insert(shard.to_string(), queue);
        println!("[Snerd] Acquired and started {}", shard);
    }

    async fn reconcile_shards(&self, current_owned: &[String]) {
        let mut shards = self.shards.write().await;
        let existing: Vec<String> = shards.keys().cloned().collect();
        for shard in existing {
            if !current_owned.contains(&shard) {
                // We lost this shard, remove it. 
                // The `SnerdQueue` will be dropped, dropping its `_storage_lock`.
                shards.remove(&shard);
                println!("[Snerd] Lost lease for {}, stopped engine.", shard);
            }
        }
    }

    pub async fn shutdown(&self) {
        self.stop_flag.store(true, std::sync::atomic::Ordering::Relaxed);
        let mut shards = self.shards.write().await;
        for (shard, queue) in shards.iter() {
            queue.stop_flag.store(true, std::sync::atomic::Ordering::Relaxed);
            println!("[Snerd] Shutdown: released {}", shard);
        }
        shards.clear();
    }
}
