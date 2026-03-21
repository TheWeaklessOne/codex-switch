use std::time::Duration;

#[derive(Debug, Clone)]
pub struct SchedulerSettings {
    pub scheduler_lock_ttl: Duration,
    pub worker_heartbeat_interval: Duration,
    pub worker_lease_ttl: Duration,
    pub scheduler_poll_interval: Duration,
    pub quota_refresh_interval: Duration,
    pub gc_interval: Duration,
    pub dispatch_batch_size: usize,
    pub max_active_runs_per_identity: u32,
    pub requeue_orphaned_runs: bool,
    pub allow_oversubscribe_when_pool_full: bool,
    pub completed_worktree_ttl: Duration,
    pub failed_worktree_ttl: Duration,
    pub immediate_cleanup_terminal_failures: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerControlRecord {
    pub scheduler_v1_enabled: bool,
    pub last_quota_refresh_at: Option<i64>,
    pub last_quota_refresh_error: Option<String>,
    pub last_gc_at: Option<i64>,
    pub last_gc_error: Option<String>,
    pub updated_at: i64,
}

impl Default for SchedulerSettings {
    fn default() -> Self {
        Self {
            scheduler_lock_ttl: Duration::from_secs(30),
            worker_heartbeat_interval: Duration::from_secs(2),
            worker_lease_ttl: Duration::from_secs(15),
            scheduler_poll_interval: Duration::from_secs(2),
            quota_refresh_interval: Duration::from_secs(60),
            gc_interval: Duration::from_secs(300),
            dispatch_batch_size: 16,
            max_active_runs_per_identity: 1,
            requeue_orphaned_runs: true,
            allow_oversubscribe_when_pool_full: false,
            completed_worktree_ttl: Duration::from_secs(60 * 60 * 24),
            failed_worktree_ttl: Duration::from_secs(60 * 60),
            immediate_cleanup_terminal_failures: false,
        }
    }
}
