use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};
use tokio::sync::broadcast;

use crate::queue::SnerdQueue;
use crate::sharded_queue::SnerdShardedQueue;

/// A single entry in the dashboard's Progress Stream.
struct ProgressEvent {
    ts: f64,
    task_id: String,
    data: String,
}

type ProgressRing = Arc<Mutex<VecDeque<ProgressEvent>>>;

const RING_CAP: usize = 500;

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Reads the append-only task log and returns the latest line per taskId
/// (cron refires and retries append new lines over time).
fn read_deduped_tasks_multiple(tasks_paths: &[String]) -> HashMap<String, Value> {
    let mut tasks_map: HashMap<String, Value> = HashMap::new();
    for tasks_path in tasks_paths {
        let file = match std::fs::File::open(tasks_path) {
            Ok(f) => f,
            Err(_) => continue,
        };
        for line in BufReader::new(file).lines().map_while(Result::ok) {
            if line.trim().is_empty() {
                continue;
            }
            if let Ok(t) = serde_json::from_str::<Value>(&line) {
                if let Some(tid) = t.get("taskId").and_then(|v| v.as_str()) {
                    tasks_map.insert(tid.to_string(), t);
                }
            }
        }
    }
    tasks_map
}

fn read_deduped_tasks(tasks_path: &str) -> HashMap<String, Value> {
    read_deduped_tasks_multiple(&[tasks_path.to_string()])
}


fn has_job_error(t: &Value) -> bool {
    // Tolerate the lowercase variant in case logs were written by older builds
    t.get("LastJobError").is_some() || t.get("lastJobError").is_some()
}

/// Derives the UI status for a deduped task record.
fn dashboard_status(t: &Value) -> &'static str {
    let has_err = has_job_error(t);
    let deleted = t
        .get("deletedAt")
        .map_or(false, |v| !v.is_null());
    if deleted {
        let retry_count = t.get("retryCount").and_then(|v| v.as_i64()).unwrap_or(0);
        let max_retries = t.get("maxRetries").and_then(|v| v.as_i64()).unwrap_or(0);
        if has_err && retry_count >= max_retries {
            return "dead_letter";
        } else if has_err {
            return "failed";
        }
        return "completed";
    }
    if has_err {
        return "failed";
    }
    if let Some(exec_at) = t.get("executeAt").and_then(|v| v.as_str()) {
        if let Ok(et) = chrono::DateTime::parse_from_rfc3339(exec_at) {
            if et <= chrono::Utc::now() {
                return "active";
            }
        }
    }
    "queued"
}

fn stats_body_multiple(tasks_paths: &[String]) -> String {
    let tasks_map = read_deduped_tasks_multiple(tasks_paths);
    let enqueued = tasks_map.len();
    let mut processed = 0usize;
    let mut failed = 0usize;
    for t in tasks_map.values() {
        let deleted = t.get("deletedAt").map_or(false, |v| !v.is_null());
        if deleted {
            if has_job_error(t) {
                failed += 1;
            } else {
                processed += 1;
            }
        }
    }
    format!(
        "{{\"enqueued\":{},\"processed\":{},\"failed\":{}}}",
        enqueued, processed, failed
    )
}

fn stats_body(tasks_path: &str) -> String {
    stats_body_multiple(&[tasks_path.to_string()])
}

fn tasks_body_multiple(tasks_paths: &[String]) -> String {
    let tasks_map = read_deduped_tasks_multiple(tasks_paths);
    let res: Vec<Value> = tasks_map
        .values()
        .map(|t| {
            json!({
                "id": t.get("taskId"),
                "type": t.get("taskType"),
                "status": dashboard_status(t),
                "progress": 0,
                "retryCount": t.get("retryCount").and_then(|v| v.as_i64()).unwrap_or(0),
                "maxRetries": t.get("maxRetries").and_then(|v| v.as_i64()).unwrap_or(0),
                "retryAfterTime": t.get("retryAfterTime").and_then(|v| v.as_str()).unwrap_or(""),
                "cronExpression": t.get("cronExpression"),
                "webhookUrl": t.get("webhookUrl"),
                "maxExecutionSeconds": t.get("maxExecutionSeconds"),
            })
        })
        .collect();
    serde_json::to_string(&res).unwrap_or_else(|_| "[]".to_string())
}

fn tasks_body(tasks_path: &str) -> String {
    tasks_body_multiple(&[tasks_path.to_string()])
}

fn progress_body(ring: &ProgressRing) -> String {
    let events: Vec<Value> = {
        let r = ring.lock().unwrap();
        let skip = r.len().saturating_sub(100);
        r.iter()
            .skip(skip)
            .map(|ev| {
                json!({
                    "ts": ev.ts,
                    "task_id": ev.task_id,
                    "data": ev.data,
                })
            })
            .collect()
    };
    serde_json::to_string(&events).unwrap_or_else(|_| "[]".to_string())
}

fn handle_connection(mut stream: TcpStream, tasks_paths: &[String], ring: &ProgressRing) {
    let mut buf = [0u8; 4096];
    let n = match stream.read(&mut buf) {
        Ok(n) if n > 0 => n,
        _ => return,
    };
    let req = String::from_utf8_lossy(&buf[..n]);
    let first_line = req.lines().next().unwrap_or("");
    let mut parts = first_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let target = parts.next().unwrap_or("/");
    let path = target.split('?').next().unwrap_or("/");

    let (status, content_type, body) = if method != "GET" {
        (
            "405 Method Not Allowed",
            "text/plain",
            "Method Not Allowed".to_string(),
        )
    } else {
        match path {
            "/api/stats" => ("200 OK", "application/json", stats_body_multiple(tasks_paths)),
            "/api/tasks" => ("200 OK", "application/json", tasks_body_multiple(tasks_paths)),
            "/api/progress" => ("200 OK", "application/json", progress_body(ring)),
            "/" => match std::fs::read_to_string("static/index.html") {
                Ok(html) => ("200 OK", "text/html", html),
                Err(_) => (
                    "404 Not Found",
                    "text/plain",
                    "Dashboard UI not found: place the dashboard bundle at ./static/index.html"
                        .to_string(),
                ),
            },
            _ => ("404 Not Found", "text/plain", "Not Found".to_string()),
        }
    };

    let resp = format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status,
        content_type,
        body.len(),
        body
    );
    let _ = stream.write_all(resp.as_bytes());
}

impl SnerdQueue {
    /// Starts the built-in dashboard UI on the given port.
    ///
    /// The dashboard is a single-page React app (served from ./static/index.html
    /// relative to the process working directory) that shows live queue stats,
    /// a Recent Jobs table, and a real-time Progress Stream fed by yield_progress.
    /// Updates are delivered via HTTP polling of the JSON API (/api/stats,
    /// /api/tasks, /api/progress).
    ///
    /// The dashboard only serves the UI — jobs keep running whether or not it is open.
    pub fn start_dashboard(&self, port: u16) {
        let ring: ProgressRing = Arc::new(Mutex::new(VecDeque::with_capacity(RING_CAP)));

        // Feed the progress stream from the queue's internal broadcast channel
        let mut rx = self.subscribe_progress();
        let feeder_ring = Arc::clone(&ring);
        std::thread::spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
            {
                Ok(rt) => rt,
                Err(_) => return,
            };
            rt.block_on(async move {
                loop {
                    match rx.recv().await {
                        Ok(msg) => {
                            let mut r = feeder_ring.lock().unwrap();
                            r.push_back(ProgressEvent {
                                ts: now_secs(),
                                task_id: msg.task_id,
                                data: msg.data,
                            });
                            if r.len() > RING_CAP {
                                r.pop_front();
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            });
        });

        let tasks_path = self.file_store.file_path().to_string_lossy().to_string();
        let listener = match TcpListener::bind(format!("0.0.0.0:{}", port)) {
            Ok(l) => l,
            Err(e) => {
                println!("[Snerd] Failed to start dashboard on port {}: {}", port, e);
                return;
            }
        };

        println!("[Snerd] Dashboard running on http://localhost:{}", port);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                if let Ok(stream) = stream {
                    let path = vec![tasks_path.clone()];
                    let r = Arc::clone(&ring);
                    std::thread::spawn(move || handle_connection(stream, &path, &r));
                }
            }
        });
    }
}

impl SnerdShardedQueue {
    pub fn start_dashboard(&self, port: u16) {
        let ring: ProgressRing = Arc::new(Mutex::new(VecDeque::with_capacity(RING_CAP)));
        let mut rx = self.subscribe_progress();
        let feeder_ring = Arc::clone(&ring);
        std::thread::spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread().enable_time().build() {
                Ok(rt) => rt,
                Err(_) => return,
            };
            rt.block_on(async move {
                loop {
                    match rx.recv().await {
                        Ok(msg) => {
                            let mut r = feeder_ring.lock().unwrap();
                            r.push_back(ProgressEvent {
                                ts: now_secs(),
                                task_id: msg.task_id,
                                data: msg.data,
                            });
                            if r.len() > RING_CAP {
                                r.pop_front();
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            });
        });

        let listener = match TcpListener::bind(format!("0.0.0.0:{}", port)) {
            Ok(l) => l,
            Err(e) => {
                println!("[Snerd] Failed to start dashboard on port {}: {}", port, e);
                return;
            }
        };

        println!("[Snerd] Dashboard running on http://localhost:{}", port);
        
        let sq = self.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
            for stream in listener.incoming() {
                if let Ok(stream) = stream {
                    let r = Arc::clone(&ring);
                    // Fetch all paths for owned shards
                    let paths = rt.block_on(async {
                        let shards = sq.get_shards().await;
                        shards.values().map(|q| q.file_store.file_path().to_string_lossy().to_string()).collect::<Vec<_>>()
                    });
                    
                    std::thread::spawn(move || handle_connection(stream, &paths, &r));
                }
            }
        });
    }
}
