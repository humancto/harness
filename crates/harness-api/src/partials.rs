//! In-memory per-task ring buffers for streaming partial output
//! (3.2-stream, ADR-0020).
//!
//! One [`PartialBuffers`] per daemon, shared between:
//!
//! - the daemon's partial-result plumbing (writer): the local execution
//!   path appends frames directly (no wire hop), and the issuer-side
//!   `on_partial` handler appends frames received over
//!   `harness.task.partial`;
//! - `GET /api/v1/tasks/{id}` (reader): the response's `partials` array
//!   is the ring's current contents.
//!
//! Bounds (ADR-0020): at most [`RING_CAPACITY`] frames per task
//! (oldest evicted first — `seq` keeps counting so consumers can see
//! the gap) and at most [`MAX_TRACKED_TASKS`] task entries total
//! (oldest-created task entry evicted first). Partials are best-effort
//! progress; the stored terminal result is authoritative, so eviction
//! loses nothing durable.

use std::collections::{HashMap, VecDeque};

use harness_core::TaskId;
use parking_lot::Mutex;
use serde::Serialize;

/// Frames retained per task.
pub const RING_CAPACITY: usize = 500;

/// Task entries retained across the daemon. Oldest-created evicted.
pub const MAX_TRACKED_TASKS: usize = 256;

/// One buffered line frame, as served in the API `partials` array.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PartialFrame {
    /// Per-task append counter, assigned by this buffer. Monotonic from
    /// 0; after ring eviction the retained frames' `seq` values reveal
    /// how many were dropped.
    pub seq: u64,
    /// `"stdout"` | `"stderr"`.
    pub stream: String,
    pub line: String,
}

#[derive(Debug, Default)]
struct TaskRing {
    next_seq: u64,
    frames: VecDeque<PartialFrame>,
}

#[derive(Debug, Default)]
struct Inner {
    rings: HashMap<TaskId, TaskRing>,
    /// Creation order for task-entry eviction.
    order: VecDeque<TaskId>,
}

/// See the module docs.
#[derive(Debug, Default)]
pub struct PartialBuffers {
    inner: Mutex<Inner>,
}

impl PartialBuffers {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append one frame to `task`'s ring, evicting the oldest frame at
    /// [`RING_CAPACITY`] and the oldest task entry at
    /// [`MAX_TRACKED_TASKS`]. Non-blocking (short critical section) —
    /// callable from recv loops and capability reader tasks.
    pub fn append(&self, task: TaskId, stream: &str, line: String) {
        let mut inner = self.inner.lock();
        if !inner.rings.contains_key(&task) {
            if inner.order.len() >= MAX_TRACKED_TASKS {
                if let Some(evict) = inner.order.pop_front() {
                    inner.rings.remove(&evict);
                }
            }
            inner.order.push_back(task);
            inner.rings.insert(task, TaskRing::default());
        }
        // Entry exists by construction above.
        let Some(ring) = inner.rings.get_mut(&task) else {
            return;
        };
        let seq = ring.next_seq;
        ring.next_seq += 1;
        if ring.frames.len() >= RING_CAPACITY {
            ring.frames.pop_front();
        }
        ring.frames.push_back(PartialFrame {
            seq,
            stream: stream.to_string(),
            line,
        });
    }

    /// Snapshot of `task`'s buffered frames, oldest first. Empty when
    /// the task has streamed nothing (or its entry was evicted).
    #[must_use]
    pub fn frames(&self, task: TaskId) -> Vec<PartialFrame> {
        self.inner
            .lock()
            .rings
            .get(&task)
            .map(|r| r.frames.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Total frames ever appended for `task` (including evicted ones).
    #[must_use]
    pub fn total_appended(&self, task: TaskId) -> u64 {
        self.inner.lock().rings.get(&task).map_or(0, |r| r.next_seq)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn append_preserves_order_and_seq() {
        let b = PartialBuffers::new();
        let t = TaskId::new_v7();
        b.append(t, "stdout", "one".into());
        b.append(t, "stderr", "two".into());
        let frames = b.frames(t);
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].seq, 0);
        assert_eq!(frames[0].stream, "stdout");
        assert_eq!(frames[0].line, "one");
        assert_eq!(frames[1].seq, 1);
        assert_eq!(frames[1].stream, "stderr");
    }

    #[test]
    fn ring_evicts_oldest_at_capacity() {
        let b = PartialBuffers::new();
        let t = TaskId::new_v7();
        for i in 0..(RING_CAPACITY + 100) {
            b.append(t, "stdout", format!("line-{i}"));
        }
        let frames = b.frames(t);
        assert_eq!(frames.len(), RING_CAPACITY);
        // Oldest 100 evicted; seq keeps counting so the gap is visible.
        assert_eq!(frames[0].seq, 100);
        assert_eq!(frames[0].line, "line-100");
        assert_eq!(frames.last().unwrap().seq, (RING_CAPACITY + 100 - 1) as u64);
        assert_eq!(b.total_appended(t), (RING_CAPACITY + 100) as u64);
    }

    #[test]
    fn task_entries_evicted_oldest_first() {
        let b = PartialBuffers::new();
        let first = TaskId::new_v7();
        b.append(first, "stdout", "x".into());
        for _ in 0..MAX_TRACKED_TASKS {
            b.append(TaskId::new_v7(), "stdout", "y".into());
        }
        assert!(
            b.frames(first).is_empty(),
            "oldest task entry must be evicted"
        );
    }

    #[test]
    fn unknown_task_yields_empty() {
        let b = PartialBuffers::new();
        assert!(b.frames(TaskId::new_v7()).is_empty());
        assert_eq!(b.total_appended(TaskId::new_v7()), 0);
    }
}
