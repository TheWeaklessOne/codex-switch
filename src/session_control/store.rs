use std::path::{Path, PathBuf};
use std::time::Duration;

use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde_json::Value;

use crate::domain::identity::IdentityId;
use crate::error::{AppError, Result};
use crate::session_control::domain::{
    ContinuityMode, HandoffFallbackMode, HandoffLeaseStateKind, SessionEventRecord,
    SessionHandoffId, SessionHandoffRecord, SessionHandoffStatus, SessionId, SessionRecord,
    SessionSnapshot, SessionStatus, SessionTurnId, SessionTurnRecord, SessionTurnStatus,
};
use crate::storage::paths::{ensure_directory, session_control_db_path, session_control_root_path};

const INTERFACE_VERSION: &str = "1";

pub struct AppendEvent<'a> {
    pub session_id: &'a SessionId,
    pub thread_id: &'a str,
    pub turn_id: Option<&'a SessionTurnId>,
    pub runtime_turn_id: Option<&'a str>,
    pub handoff_id: Option<&'a SessionHandoffId>,
    pub event: &'a str,
    pub timestamp: i64,
    pub payload: &'a Value,
}

const SCHEMA_VERSION: i64 = 2;

const MIGRATION_V2: &str = r#"
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    topic_key TEXT NOT NULL UNIQUE,
    workspace_root TEXT NOT NULL,
    model TEXT,
    thread_id TEXT NOT NULL,
    current_identity_id TEXT NOT NULL,
    current_identity_name TEXT NOT NULL,
    status TEXT NOT NULL,
    last_turn_id TEXT,
    active_turn_id TEXT,
    continuity_mode TEXT NOT NULL,
    safe_to_continue INTEGER NOT NULL,
    pending_handoff_id TEXT,
    last_checkpoint_id TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS session_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    session_id TEXT NOT NULL UNIQUE,
    created_at INTEGER NOT NULL,
    FOREIGN KEY(session_id) REFERENCES sessions(session_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS turns (
    turn_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    identity_id TEXT NOT NULL,
    identity_name TEXT NOT NULL,
    status TEXT NOT NULL,
    prompt_text TEXT NOT NULL,
    continuity_mode TEXT NOT NULL,
    runtime_turn_id TEXT,
    started_at INTEGER,
    finished_at INTEGER,
    failure_kind TEXT,
    failure_message TEXT,
    worker_owner_id TEXT,
    worker_pid INTEGER,
    heartbeat_at INTEGER,
    heartbeat_expires_at INTEGER,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    lease_thread_id TEXT,
    lease_token TEXT,
    lease_persist_on_finish INTEGER NOT NULL DEFAULT 0,
    idempotency_key TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    max_runtime_secs INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY(session_id) REFERENCES sessions(session_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS handoffs (
    handoff_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    from_identity_id TEXT NOT NULL,
    from_identity_name TEXT NOT NULL,
    to_identity_id TEXT NOT NULL,
    to_identity_name TEXT NOT NULL,
    status TEXT NOT NULL,
    lease_token TEXT NOT NULL,
    lease_owner_identity_id TEXT NOT NULL,
    lease_owner_identity_name TEXT NOT NULL,
    lease_state_kind TEXT NOT NULL,
    reason TEXT NOT NULL,
    baseline_turn_id TEXT,
    observed_turn_id TEXT,
    fallback_mode TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY(session_id) REFERENCES sessions(session_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS handoff_idempotency (
    session_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    handoff_id TEXT NOT NULL UNIQUE,
    created_at INTEGER NOT NULL,
    PRIMARY KEY(session_id, idempotency_key),
    FOREIGN KEY(session_id) REFERENCES sessions(session_id) ON DELETE RESTRICT,
    FOREIGN KEY(handoff_id) REFERENCES handoffs(handoff_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS session_events (
    sequence_no INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    turn_id TEXT,
    runtime_turn_id TEXT,
    handoff_id TEXT,
    event TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY(session_id) REFERENCES sessions(session_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_session_events_session_sequence
    ON session_events(session_id, sequence_no);
CREATE UNIQUE INDEX IF NOT EXISTS idx_turns_active_session
    ON turns(session_id)
    WHERE status IN ('queued', 'starting', 'running');
CREATE UNIQUE INDEX IF NOT EXISTS idx_turns_idempotency
    ON turns(session_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
"#;

pub struct SessionControlStore {
    connection: Connection,
    _base_root: PathBuf,
}

impl SessionControlStore {
    pub fn open(base_root: &Path) -> Result<Self> {
        ensure_directory(&session_control_root_path(base_root), 0o700)?;
        let connection = Connection::open(session_control_db_path(base_root))?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "NORMAL")?;
        let store = Self {
            connection,
            _base_root: base_root.to_path_buf(),
        };
        store.run_migrations()?;
        Ok(store)
    }

    fn run_migrations(&self) -> Result<()> {
        self.connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY
            );",
        )?;
        let version: Option<i64> =
            self.connection
                .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
                    row.get(0)
                })?;
        if version == Some(SCHEMA_VERSION) {
            return Ok(());
        }
        self.connection.execute_batch(
            "DROP TABLE IF EXISTS handoff_idempotency;
             DROP TABLE IF EXISTS session_idempotency;
             DROP TABLE IF EXISTS session_events;
             DROP TABLE IF EXISTS handoffs;
             DROP TABLE IF EXISTS turns;
             DROP TABLE IF EXISTS sessions;
             DELETE FROM schema_migrations;",
        )?;
        self.connection.execute_batch(MIGRATION_V2)?;
        self.connection.execute(
            "INSERT INTO schema_migrations(version) VALUES(?1)",
            params![SCHEMA_VERSION],
        )?;
        Ok(())
    }

    pub fn create_session(
        &mut self,
        record: &SessionRecord,
        idempotency_key: Option<&str>,
    ) -> Result<()> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT INTO sessions(
                session_id, topic_key, workspace_root, model, thread_id, current_identity_id,
                current_identity_name, status, last_turn_id, active_turn_id, continuity_mode,
                safe_to_continue, pending_handoff_id, last_checkpoint_id, created_at, updated_at
             ) VALUES(
                ?1, ?2, ?3, ?4, ?5, ?6,
                ?7, ?8, ?9, ?10, ?11,
                ?12, ?13, ?14, ?15, ?16
             )",
            params![
                record.session_id.as_str(),
                record.topic_key.as_str(),
                record.workspace_root.as_str(),
                record.model.as_deref(),
                record.thread_id.as_str(),
                record.current_identity_id.as_str(),
                record.current_identity_name.as_str(),
                record.status.as_str(),
                record.last_turn_id.as_ref().map(SessionTurnId::as_str),
                record.active_turn_id.as_ref().map(SessionTurnId::as_str),
                record.continuity_mode.as_str(),
                if record.safe_to_continue { 1 } else { 0 },
                record
                    .pending_handoff_id
                    .as_ref()
                    .map(SessionHandoffId::as_str),
                record.last_checkpoint_id.as_deref(),
                record.created_at,
                record.updated_at,
            ],
        )?;
        if let Some(idempotency_key) = idempotency_key {
            insert_session_idempotency_tx(
                &tx,
                record.session_id.as_str(),
                idempotency_key,
                record.created_at,
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn create_turn_and_activate_session(
        &mut self,
        turn: &SessionTurnRecord,
        session_status: SessionStatus,
    ) -> Result<()> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let session = load_session_tx(&tx, turn.session_id.as_str())?.ok_or_else(|| {
            AppError::SessionNotFound {
                session_id: turn.session_id.to_string(),
            }
        })?;
        if let Some(active_turn_id) = session.active_turn_id {
            return Err(AppError::SessionTurnAlreadyActive {
                session_id: session.session_id.to_string(),
                active_turn_suffix: format!(" ({active_turn_id})"),
            });
        }
        insert_turn_tx(&tx, turn)?;
        tx.execute(
            "UPDATE sessions
             SET status = ?2,
                 active_turn_id = ?3,
                 continuity_mode = ?4,
                 safe_to_continue = 0,
                 updated_at = ?5
             WHERE session_id = ?1",
            params![
                session.session_id.as_str(),
                session_status.as_str(),
                turn.turn_id.as_str(),
                turn.continuity_mode.as_str(),
                turn.updated_at,
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn create_handoff_and_update_session(
        &mut self,
        handoff: &SessionHandoffRecord,
        idempotency_key: Option<&str>,
        session_status: SessionStatus,
        continuity_mode: ContinuityMode,
        safe_to_continue: bool,
    ) -> Result<()> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let session = load_session_tx(&tx, handoff.session_id.as_str())?.ok_or_else(|| {
            AppError::SessionNotFound {
                session_id: handoff.session_id.to_string(),
            }
        })?;
        if let Some(existing_handoff_id) = session.pending_handoff_id {
            return Err(AppError::SessionHandoffPending {
                session_id: session.session_id.to_string(),
                handoff_id: existing_handoff_id.to_string(),
            });
        }
        save_handoff_tx(&tx, handoff)?;
        if let Some(idempotency_key) = idempotency_key {
            insert_handoff_idempotency_tx(
                &tx,
                handoff.session_id.as_str(),
                handoff.handoff_id.as_str(),
                idempotency_key,
                handoff.created_at,
            )?;
        }
        tx.execute(
            "UPDATE sessions
             SET status = ?2,
                 continuity_mode = ?3,
                 safe_to_continue = ?4,
                 pending_handoff_id = ?5,
                 updated_at = ?6
             WHERE session_id = ?1",
            params![
                session.session_id.as_str(),
                session_status.as_str(),
                continuity_mode.as_str(),
                if safe_to_continue { 1 } else { 0 },
                handoff.handoff_id.as_str(),
                handoff.updated_at,
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn save_session(&mut self, record: &SessionRecord) -> Result<()> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        save_session_tx(&tx, record)?;
        tx.commit()?;
        Ok(())
    }

    pub fn save_turn(&mut self, record: &SessionTurnRecord) -> Result<()> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        save_turn_tx(&tx, record)?;
        tx.commit()?;
        Ok(())
    }

    pub fn save_handoff(&mut self, record: &SessionHandoffRecord) -> Result<()> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        save_handoff_tx(&tx, record)?;
        tx.commit()?;
        Ok(())
    }

    pub fn save_session_and_turn(
        &mut self,
        session: &SessionRecord,
        turn: &SessionTurnRecord,
    ) -> Result<()> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        save_session_tx(&tx, session)?;
        save_turn_tx(&tx, turn)?;
        tx.commit()?;
        Ok(())
    }

    pub fn save_session_and_handoff(
        &mut self,
        session: &SessionRecord,
        handoff: &SessionHandoffRecord,
    ) -> Result<()> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        save_session_tx(&tx, session)?;
        save_handoff_tx(&tx, handoff)?;
        tx.commit()?;
        Ok(())
    }

    pub fn save_session_turn_and_handoff(
        &mut self,
        session: &SessionRecord,
        turn: &SessionTurnRecord,
        handoff: Option<&SessionHandoffRecord>,
    ) -> Result<()> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        save_session_tx(&tx, session)?;
        save_turn_tx(&tx, turn)?;
        if let Some(handoff) = handoff {
            save_handoff_tx(&tx, handoff)?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn mark_turn_worker_spawned(
        &mut self,
        turn_id: &str,
        worker_owner_id: &str,
        worker_pid: u32,
        heartbeat_at: i64,
        heartbeat_expires_at: i64,
    ) -> Result<()> {
        self.connection.execute(
            "UPDATE turns
             SET worker_owner_id = ?2,
                 worker_pid = ?3,
                 heartbeat_at = ?4,
                 heartbeat_expires_at = ?5,
                 updated_at = ?4
             WHERE turn_id = ?1",
            params![
                turn_id,
                worker_owner_id,
                worker_pid,
                heartbeat_at,
                heartbeat_expires_at
            ],
        )?;
        Ok(())
    }

    pub fn heartbeat_turn(
        &mut self,
        turn_id: &str,
        worker_owner_id: &str,
        heartbeat_at: i64,
        heartbeat_expires_at: i64,
    ) -> Result<bool> {
        let rows = self.connection.execute(
            "UPDATE turns
             SET heartbeat_at = ?3,
                 heartbeat_expires_at = ?4,
                 updated_at = ?3
             WHERE turn_id = ?1
               AND worker_owner_id = ?2
               AND status IN ('starting', 'running')",
            params![turn_id, worker_owner_id, heartbeat_at, heartbeat_expires_at],
        )?;
        Ok(rows > 0)
    }

    pub fn load_session(&self, session_id: &str) -> Result<Option<SessionRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT session_id, topic_key, workspace_root, model, thread_id, current_identity_id,
                    current_identity_name, status, last_turn_id, active_turn_id, continuity_mode,
                    safe_to_continue, pending_handoff_id, last_checkpoint_id, created_at, updated_at
             FROM sessions
             WHERE session_id = ?1",
        )?;
        statement
            .query_row(params![session_id], session_from_row)
            .optional()
            .map_err(Into::into)
    }

    pub fn load_session_by_topic_key(&self, topic_key: &str) -> Result<Option<SessionRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT session_id, topic_key, workspace_root, model, thread_id, current_identity_id,
                    current_identity_name, status, last_turn_id, active_turn_id, continuity_mode,
                    safe_to_continue, pending_handoff_id, last_checkpoint_id, created_at, updated_at
             FROM sessions
             WHERE topic_key = ?1",
        )?;
        statement
            .query_row(params![topic_key], session_from_row)
            .optional()
            .map_err(Into::into)
    }

    pub fn load_session_by_idempotency(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<SessionRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT s.session_id, s.topic_key, s.workspace_root, s.model, s.thread_id,
                    s.current_identity_id, s.current_identity_name, s.status, s.last_turn_id,
                    s.active_turn_id, s.continuity_mode, s.safe_to_continue, s.pending_handoff_id,
                    s.last_checkpoint_id, s.created_at, s.updated_at
             FROM sessions s
             JOIN session_idempotency i ON i.session_id = s.session_id
             WHERE i.idempotency_key = ?1",
        )?;
        statement
            .query_row(params![idempotency_key], session_from_row)
            .optional()
            .map_err(Into::into)
    }

    pub fn list_sessions(&self) -> Result<Vec<SessionRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT session_id, topic_key, workspace_root, model, thread_id, current_identity_id,
                    current_identity_name, status, last_turn_id, active_turn_id, continuity_mode,
                    safe_to_continue, pending_handoff_id, last_checkpoint_id, created_at, updated_at
             FROM sessions
             ORDER BY updated_at DESC, session_id DESC",
        )?;
        let rows = statement.query_map([], session_from_row)?;
        let mut sessions = Vec::new();
        for row in rows {
            sessions.push(row?);
        }
        Ok(sessions)
    }

    pub fn load_turn(&self, turn_id: &str) -> Result<Option<SessionTurnRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT turn_id, session_id, thread_id, identity_id, identity_name, status, prompt_text,
                    continuity_mode, runtime_turn_id, started_at, finished_at, failure_kind,
                    failure_message, worker_owner_id, worker_pid, heartbeat_at,
                    heartbeat_expires_at, cancel_requested, lease_thread_id, lease_token,
                    lease_persist_on_finish, idempotency_key, created_at, updated_at, max_runtime_secs
             FROM turns
             WHERE turn_id = ?1",
        )?;
        statement
            .query_row(params![turn_id], turn_from_row)
            .optional()
            .map_err(Into::into)
    }

    pub fn find_turn_by_session_idempotency(
        &self,
        session_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<SessionTurnRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT turn_id, session_id, thread_id, identity_id, identity_name, status, prompt_text,
                    continuity_mode, runtime_turn_id, started_at, finished_at, failure_kind,
                    failure_message, worker_owner_id, worker_pid, heartbeat_at,
                    heartbeat_expires_at, cancel_requested, lease_thread_id, lease_token,
                    lease_persist_on_finish, idempotency_key, created_at, updated_at, max_runtime_secs
             FROM turns
             WHERE session_id = ?1 AND idempotency_key = ?2",
        )?;
        statement
            .query_row(params![session_id, idempotency_key], turn_from_row)
            .optional()
            .map_err(Into::into)
    }

    pub fn load_handoff(&self, handoff_id: &str) -> Result<Option<SessionHandoffRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT handoff_id, session_id, thread_id, from_identity_id, from_identity_name,
                    to_identity_id, to_identity_name, status, lease_token,
                    lease_owner_identity_id, lease_owner_identity_name, lease_state_kind, reason,
                    baseline_turn_id, observed_turn_id, fallback_mode, created_at, updated_at
             FROM handoffs
             WHERE handoff_id = ?1",
        )?;
        statement
            .query_row(params![handoff_id], handoff_from_row)
            .optional()
            .map_err(Into::into)
    }

    pub fn load_handoff_by_session_idempotency(
        &self,
        session_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<SessionHandoffRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT h.handoff_id, h.session_id, h.thread_id, h.from_identity_id,
                    h.from_identity_name, h.to_identity_id, h.to_identity_name, h.status,
                    h.lease_token, h.lease_owner_identity_id, h.lease_owner_identity_name,
                    h.lease_state_kind, h.reason, h.baseline_turn_id, h.observed_turn_id,
                    h.fallback_mode, h.created_at, h.updated_at
             FROM handoffs h
             JOIN handoff_idempotency i ON i.handoff_id = h.handoff_id
             WHERE i.session_id = ?1 AND i.idempotency_key = ?2",
        )?;
        statement
            .query_row(params![session_id, idempotency_key], handoff_from_row)
            .optional()
            .map_err(Into::into)
    }

    pub fn load_session_snapshot(&self, session_id: &str) -> Result<Option<SessionSnapshot>> {
        let Some(session) = self.load_session(session_id)? else {
            return Ok(None);
        };
        let active_turn = match session.active_turn_id.as_ref() {
            Some(turn_id) => self.load_turn(turn_id.as_str())?,
            None => None,
        };
        let last_turn = match session.last_turn_id.as_ref() {
            Some(turn_id)
                if active_turn
                    .as_ref()
                    .is_some_and(|active| active.turn_id == *turn_id) =>
            {
                active_turn.clone()
            }
            Some(turn_id) => self.load_turn(turn_id.as_str())?,
            None => None,
        };
        let pending_handoff = match session.pending_handoff_id.as_ref() {
            Some(handoff_id) => self.load_handoff(handoff_id.as_str())?,
            None => None,
        };
        Ok(Some(SessionSnapshot {
            session,
            active_turn,
            last_turn,
            pending_handoff,
        }))
    }

    pub fn append_event(&mut self, event: AppendEvent<'_>) -> Result<SessionEventRecord> {
        let payload_json = serde_json::to_string(event.payload)?;
        self.connection.execute(
            "INSERT INTO session_events(
                session_id, thread_id, turn_id, runtime_turn_id, handoff_id, event, payload_json, created_at
             ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                event.session_id.as_str(),
                event.thread_id,
                event.turn_id.map(SessionTurnId::as_str),
                event.runtime_turn_id,
                event.handoff_id.map(SessionHandoffId::as_str),
                event.event,
                payload_json,
                event.timestamp
            ],
        )?;
        let sequence_no = self.connection.last_insert_rowid();
        Ok(SessionEventRecord {
            sequence_no,
            interface_version: INTERFACE_VERSION.to_string(),
            event: event.event.to_string(),
            session_id: event.session_id.clone(),
            thread_id: event.thread_id.to_string(),
            turn_id: event.turn_id.cloned(),
            runtime_turn_id: event.runtime_turn_id.map(ToString::to_string),
            handoff_id: event.handoff_id.cloned(),
            timestamp: event.timestamp,
            payload: event.payload.clone(),
        })
    }

    pub fn events_after(
        &self,
        session_id: &SessionId,
        after_sequence_no: i64,
    ) -> Result<Vec<SessionEventRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT sequence_no, session_id, thread_id, turn_id, runtime_turn_id, handoff_id, event, payload_json, created_at
             FROM session_events
             WHERE session_id = ?1 AND sequence_no > ?2
             ORDER BY sequence_no ASC",
        )?;
        let rows = statement.query_map(
            params![session_id.as_str(), after_sequence_no],
            event_from_row,
        )?;
        let mut events = Vec::new();
        for row in rows {
            events.push(row?);
        }
        Ok(events)
    }
}

fn load_session_tx(tx: &Transaction<'_>, session_id: &str) -> Result<Option<SessionRecord>> {
    let mut statement = tx.prepare(
        "SELECT session_id, topic_key, workspace_root, model, thread_id, current_identity_id,
                current_identity_name, status, last_turn_id, active_turn_id, continuity_mode,
                safe_to_continue, pending_handoff_id, last_checkpoint_id, created_at, updated_at
         FROM sessions
         WHERE session_id = ?1",
    )?;
    statement
        .query_row(params![session_id], session_from_row)
        .optional()
        .map_err(Into::into)
}

fn insert_turn_tx(tx: &Transaction<'_>, record: &SessionTurnRecord) -> Result<()> {
    tx.execute(
        "INSERT INTO turns(
            turn_id, session_id, thread_id, identity_id, identity_name, status, prompt_text,
            continuity_mode, runtime_turn_id, started_at, finished_at, failure_kind,
            failure_message, worker_owner_id, worker_pid, heartbeat_at, heartbeat_expires_at,
            cancel_requested, lease_thread_id, lease_token, lease_persist_on_finish,
            idempotency_key, created_at, updated_at, max_runtime_secs
         ) VALUES(
            ?1, ?2, ?3, ?4, ?5, ?6, ?7,
            ?8, ?9, ?10, ?11, ?12,
            ?13, ?14, ?15, ?16, ?17,
            ?18, ?19, ?20, ?21,
            ?22, ?23, ?24, ?25
         )",
        params![
            record.turn_id.as_str(),
            record.session_id.as_str(),
            record.thread_id.as_str(),
            record.identity_id.as_str(),
            record.identity_name.as_str(),
            record.status.as_str(),
            record.prompt_text.as_str(),
            record.continuity_mode.as_str(),
            record.runtime_turn_id.as_deref(),
            record.started_at,
            record.finished_at,
            record.failure_kind.as_deref(),
            record.failure_message.as_deref(),
            record.worker_owner_id.as_deref(),
            record.worker_pid,
            record.heartbeat_at,
            record.heartbeat_expires_at,
            if record.cancel_requested { 1 } else { 0 },
            record.lease_thread_id.as_deref(),
            record.lease_token.as_deref(),
            if record.lease_persist_on_finish { 1 } else { 0 },
            record.idempotency_key.as_deref(),
            record.created_at,
            record.updated_at,
            record.max_runtime_secs.unwrap_or(0),
        ],
    )?;
    Ok(())
}

fn save_session_tx(tx: &Transaction<'_>, record: &SessionRecord) -> Result<()> {
    tx.execute(
        "UPDATE sessions
         SET topic_key = ?2,
             workspace_root = ?3,
             model = ?4,
             thread_id = ?5,
             current_identity_id = ?6,
             current_identity_name = ?7,
             status = ?8,
             last_turn_id = ?9,
             active_turn_id = ?10,
             continuity_mode = ?11,
             safe_to_continue = ?12,
             pending_handoff_id = ?13,
             last_checkpoint_id = ?14,
             created_at = ?15,
             updated_at = ?16
         WHERE session_id = ?1",
        params![
            record.session_id.as_str(),
            record.topic_key.as_str(),
            record.workspace_root.as_str(),
            record.model.as_deref(),
            record.thread_id.as_str(),
            record.current_identity_id.as_str(),
            record.current_identity_name.as_str(),
            record.status.as_str(),
            record.last_turn_id.as_ref().map(SessionTurnId::as_str),
            record.active_turn_id.as_ref().map(SessionTurnId::as_str),
            record.continuity_mode.as_str(),
            if record.safe_to_continue { 1 } else { 0 },
            record
                .pending_handoff_id
                .as_ref()
                .map(SessionHandoffId::as_str),
            record.last_checkpoint_id.as_deref(),
            record.created_at,
            record.updated_at,
        ],
    )?;
    Ok(())
}

fn save_turn_tx(tx: &Transaction<'_>, record: &SessionTurnRecord) -> Result<()> {
    tx.execute(
        "UPDATE turns
         SET session_id = ?2,
             thread_id = ?3,
             identity_id = ?4,
             identity_name = ?5,
             status = ?6,
             prompt_text = ?7,
             continuity_mode = ?8,
             runtime_turn_id = ?9,
             started_at = ?10,
             finished_at = ?11,
             failure_kind = ?12,
             failure_message = ?13,
             worker_owner_id = ?14,
             worker_pid = ?15,
             heartbeat_at = ?16,
             heartbeat_expires_at = ?17,
             cancel_requested = ?18,
             lease_thread_id = ?19,
             lease_token = ?20,
             lease_persist_on_finish = ?21,
             idempotency_key = ?22,
             created_at = ?23,
             updated_at = ?24,
             max_runtime_secs = ?25
         WHERE turn_id = ?1",
        params![
            record.turn_id.as_str(),
            record.session_id.as_str(),
            record.thread_id.as_str(),
            record.identity_id.as_str(),
            record.identity_name.as_str(),
            record.status.as_str(),
            record.prompt_text.as_str(),
            record.continuity_mode.as_str(),
            record.runtime_turn_id.as_deref(),
            record.started_at,
            record.finished_at,
            record.failure_kind.as_deref(),
            record.failure_message.as_deref(),
            record.worker_owner_id.as_deref(),
            record.worker_pid,
            record.heartbeat_at,
            record.heartbeat_expires_at,
            if record.cancel_requested { 1 } else { 0 },
            record.lease_thread_id.as_deref(),
            record.lease_token.as_deref(),
            if record.lease_persist_on_finish { 1 } else { 0 },
            record.idempotency_key.as_deref(),
            record.created_at,
            record.updated_at,
            record.max_runtime_secs.unwrap_or(0),
        ],
    )?;
    Ok(())
}

fn save_handoff_tx(tx: &Transaction<'_>, record: &SessionHandoffRecord) -> Result<()> {
    tx.execute(
        "INSERT INTO handoffs(
            handoff_id, session_id, thread_id, from_identity_id, from_identity_name,
            to_identity_id, to_identity_name, status, lease_token, lease_owner_identity_id,
            lease_owner_identity_name, lease_state_kind, reason, baseline_turn_id,
            observed_turn_id, fallback_mode, created_at, updated_at
         ) VALUES(
            ?1, ?2, ?3, ?4, ?5,
            ?6, ?7, ?8, ?9, ?10,
            ?11, ?12, ?13, ?14, ?15,
            ?16, ?17, ?18
         )
         ON CONFLICT(handoff_id) DO UPDATE SET
            session_id = excluded.session_id,
            thread_id = excluded.thread_id,
            from_identity_id = excluded.from_identity_id,
            from_identity_name = excluded.from_identity_name,
            to_identity_id = excluded.to_identity_id,
            to_identity_name = excluded.to_identity_name,
            status = excluded.status,
            lease_token = excluded.lease_token,
            lease_owner_identity_id = excluded.lease_owner_identity_id,
            lease_owner_identity_name = excluded.lease_owner_identity_name,
            lease_state_kind = excluded.lease_state_kind,
            reason = excluded.reason,
            baseline_turn_id = excluded.baseline_turn_id,
            observed_turn_id = excluded.observed_turn_id,
            fallback_mode = excluded.fallback_mode,
            created_at = excluded.created_at,
            updated_at = excluded.updated_at",
        params![
            record.handoff_id.as_str(),
            record.session_id.as_str(),
            record.thread_id.as_str(),
            record.from_identity_id.as_str(),
            record.from_identity_name.as_str(),
            record.to_identity_id.as_str(),
            record.to_identity_name.as_str(),
            record.status.as_str(),
            record.lease_token.as_str(),
            record.lease_owner_identity_id.as_str(),
            record.lease_owner_identity_name.as_str(),
            record.lease_state_kind.as_str(),
            record.reason.as_str(),
            record.baseline_turn_id.as_deref(),
            record.observed_turn_id.as_deref(),
            record.fallback_mode.map(HandoffFallbackMode::as_str),
            record.created_at,
            record.updated_at,
        ],
    )?;
    Ok(())
}

fn insert_session_idempotency_tx(
    tx: &Transaction<'_>,
    session_id: &str,
    idempotency_key: &str,
    created_at: i64,
) -> Result<()> {
    tx.execute(
        "INSERT INTO session_idempotency(idempotency_key, session_id, created_at)
         VALUES(?1, ?2, ?3)",
        params![idempotency_key, session_id, created_at],
    )?;
    Ok(())
}

fn insert_handoff_idempotency_tx(
    tx: &Transaction<'_>,
    session_id: &str,
    handoff_id: &str,
    idempotency_key: &str,
    created_at: i64,
) -> Result<()> {
    tx.execute(
        "INSERT INTO handoff_idempotency(session_id, idempotency_key, handoff_id, created_at)
         VALUES(?1, ?2, ?3, ?4)",
        params![session_id, idempotency_key, handoff_id, created_at],
    )?;
    Ok(())
}

fn session_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionRecord> {
    Ok(SessionRecord {
        session_id: SessionId::from_string(row.get::<_, String>(0)?),
        topic_key: row.get(1)?,
        workspace_root: row.get(2)?,
        model: row.get(3)?,
        thread_id: row.get(4)?,
        current_identity_id: IdentityId::from_display_name(&row.get::<_, String>(5)?).map_err(
            |error| {
                rusqlite::Error::FromSqlConversionFailure(
                    5,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            },
        )?,
        current_identity_name: row.get(6)?,
        status: parse_session_status(row.get::<_, String>(7)?, 7)?,
        last_turn_id: row
            .get::<_, Option<String>>(8)?
            .map(SessionTurnId::from_string),
        active_turn_id: row
            .get::<_, Option<String>>(9)?
            .map(SessionTurnId::from_string),
        continuity_mode: parse_continuity_mode(row.get::<_, String>(10)?, 10)?,
        safe_to_continue: row.get::<_, i64>(11)? != 0,
        pending_handoff_id: row
            .get::<_, Option<String>>(12)?
            .map(SessionHandoffId::from_string),
        last_checkpoint_id: row.get(13)?,
        created_at: row.get(14)?,
        updated_at: row.get(15)?,
    })
}

fn turn_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionTurnRecord> {
    let max_runtime_secs = row.get::<_, i64>(24)?;
    Ok(SessionTurnRecord {
        turn_id: SessionTurnId::from_string(row.get::<_, String>(0)?),
        session_id: SessionId::from_string(row.get::<_, String>(1)?),
        thread_id: row.get(2)?,
        identity_id: IdentityId::from_display_name(&row.get::<_, String>(3)?).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                3,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        identity_name: row.get(4)?,
        status: parse_turn_status(row.get::<_, String>(5)?, 5)?,
        prompt_text: row.get(6)?,
        continuity_mode: parse_continuity_mode(row.get::<_, String>(7)?, 7)?,
        runtime_turn_id: row.get(8)?,
        started_at: row.get(9)?,
        finished_at: row.get(10)?,
        failure_kind: row.get(11)?,
        failure_message: row.get(12)?,
        worker_owner_id: row.get(13)?,
        worker_pid: row.get(14)?,
        heartbeat_at: row.get(15)?,
        heartbeat_expires_at: row.get(16)?,
        cancel_requested: row.get::<_, i64>(17)? != 0,
        lease_thread_id: row.get(18)?,
        lease_token: row.get(19)?,
        lease_persist_on_finish: row.get::<_, i64>(20)? != 0,
        idempotency_key: row.get(21)?,
        created_at: row.get(22)?,
        updated_at: row.get(23)?,
        max_runtime_secs: (max_runtime_secs > 0).then_some(max_runtime_secs),
    })
}

fn handoff_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionHandoffRecord> {
    Ok(SessionHandoffRecord {
        handoff_id: SessionHandoffId::from_string(row.get::<_, String>(0)?),
        session_id: SessionId::from_string(row.get::<_, String>(1)?),
        thread_id: row.get(2)?,
        from_identity_id: IdentityId::from_display_name(&row.get::<_, String>(3)?).map_err(
            |error| {
                rusqlite::Error::FromSqlConversionFailure(
                    3,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            },
        )?,
        from_identity_name: row.get(4)?,
        to_identity_id: IdentityId::from_display_name(&row.get::<_, String>(5)?).map_err(
            |error| {
                rusqlite::Error::FromSqlConversionFailure(
                    5,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            },
        )?,
        to_identity_name: row.get(6)?,
        status: parse_handoff_status(row.get::<_, String>(7)?, 7)?,
        lease_token: row.get(8)?,
        lease_owner_identity_id: IdentityId::from_display_name(&row.get::<_, String>(9)?).map_err(
            |error| {
                rusqlite::Error::FromSqlConversionFailure(
                    9,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            },
        )?,
        lease_owner_identity_name: row.get(10)?,
        lease_state_kind: parse_handoff_lease_state_kind(row.get::<_, String>(11)?, 11)?,
        reason: row.get(12)?,
        baseline_turn_id: row.get(13)?,
        observed_turn_id: row.get(14)?,
        fallback_mode: row
            .get::<_, Option<String>>(15)?
            .map(|value| parse_fallback_mode(value, 15))
            .transpose()?,
        created_at: row.get(16)?,
        updated_at: row.get(17)?,
    })
}

fn event_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionEventRecord> {
    Ok(SessionEventRecord {
        sequence_no: row.get(0)?,
        interface_version: INTERFACE_VERSION.to_string(),
        session_id: SessionId::from_string(row.get::<_, String>(1)?),
        thread_id: row.get(2)?,
        turn_id: row
            .get::<_, Option<String>>(3)?
            .map(SessionTurnId::from_string),
        runtime_turn_id: row.get(4)?,
        handoff_id: row
            .get::<_, Option<String>>(5)?
            .map(SessionHandoffId::from_string),
        event: row.get(6)?,
        payload: serde_json::from_str(&row.get::<_, String>(7)?).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                7,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        timestamp: row.get(8)?,
    })
}

fn parse_session_status(value: String, index: usize) -> rusqlite::Result<SessionStatus> {
    SessionStatus::parse(&value)
        .ok_or_else(|| parse_error(index, format!("invalid session status {value}")))
}

fn parse_turn_status(value: String, index: usize) -> rusqlite::Result<SessionTurnStatus> {
    SessionTurnStatus::parse(&value)
        .ok_or_else(|| parse_error(index, format!("invalid turn status {value}")))
}

fn parse_handoff_status(value: String, index: usize) -> rusqlite::Result<SessionHandoffStatus> {
    SessionHandoffStatus::parse(&value)
        .ok_or_else(|| parse_error(index, format!("invalid handoff status {value}")))
}

fn parse_continuity_mode(value: String, index: usize) -> rusqlite::Result<ContinuityMode> {
    ContinuityMode::parse(&value)
        .ok_or_else(|| parse_error(index, format!("invalid continuity mode {value}")))
}

fn parse_fallback_mode(value: String, index: usize) -> rusqlite::Result<HandoffFallbackMode> {
    HandoffFallbackMode::parse(&value)
        .ok_or_else(|| parse_error(index, format!("invalid fallback mode {value}")))
}

fn parse_handoff_lease_state_kind(
    value: String,
    index: usize,
) -> rusqlite::Result<HandoffLeaseStateKind> {
    HandoffLeaseStateKind::parse(&value)
        .ok_or_else(|| parse_error(index, format!("invalid handoff lease state kind {value}")))
}

fn parse_error(index: usize, message: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        index,
        rusqlite::types::Type::Text,
        Box::new(AppError::InvalidSessionControlState { message }),
    )
}
