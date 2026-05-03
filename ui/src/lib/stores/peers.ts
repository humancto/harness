import { writable, derived, type Readable } from "svelte/store";
import type { MeshEvent, PeerDto, PeersSnapshot } from "$lib/types";
import { fetchPeers, subscribeEvents, type EventSubscription } from "$lib/api";

interface PeersState {
  loading: boolean;
  error: string | null;
  snapshot: PeersSnapshot | null;
  /** Set when the WS has dropped and we're polling instead. */
  polling: boolean;
  /** Set once at least one snapshot has arrived. */
  hasData: boolean;
}

const initial: PeersState = {
  loading: true,
  error: null,
  snapshot: null,
  polling: false,
  hasData: false,
};

const state = writable<PeersState>(initial);
let subscription: EventSubscription | null = null;
let pollHandle: ReturnType<typeof setInterval> | null = null;
let queuedEvents: MeshEvent[] = [];
let snapshotInFlight = false;

async function refreshSnapshot() {
  if (snapshotInFlight) return;
  snapshotInFlight = true;
  try {
    const snapshot = await fetchPeers();
    state.update((s) => ({
      ...s,
      loading: false,
      error: null,
      snapshot,
      hasData: true,
    }));
    // Apply any events that arrived during the fetch (race fix per plan §6).
    const drained = queuedEvents;
    queuedEvents = [];
    for (const ev of drained) applyEvent(ev);
  } catch (err) {
    state.update((s) => ({
      ...s,
      loading: false,
      error: err instanceof Error ? err.message : String(err),
    }));
  } finally {
    snapshotInFlight = false;
  }
}

function applyEvent(event: MeshEvent) {
  state.update((s) => {
    if (!s.snapshot) {
      // No snapshot yet — buffer.
      queuedEvents.push(event);
      return s;
    }
    const snap = { ...s.snapshot, peers: [...s.snapshot.peers] };
    switch (event.type) {
      case "peer_added":
      case "peer_updated": {
        const idx = snap.peers.findIndex(
          (p) => p.node_id === event.peer.node_id,
        );
        if (idx >= 0) snap.peers[idx] = event.peer;
        else snap.peers.push(event.peer);
        snap.peers.sort((a, b) => a.node_id.localeCompare(b.node_id));
        break;
      }
      case "peer_evicted": {
        snap.peers = snap.peers.filter((p) => p.node_id !== event.node_id);
        break;
      }
      case "leader_changed": {
        snap.leader_belief = event.leader;
        snap.local = {
          ...snap.local,
          leader_belief: event.leader,
          brain_score: event.brain_score,
        };
        break;
      }
    }
    return { ...s, snapshot: snap };
  });
}

function startPolling() {
  state.update((s) => ({ ...s, polling: true }));
  pollHandle = setInterval(() => {
    void refreshSnapshot();
  }, 2000);
}

function stopPolling() {
  state.update((s) => ({ ...s, polling: false }));
  if (pollHandle) clearInterval(pollHandle);
  pollHandle = null;
}

export function startPeersStore() {
  if (subscription) return;
  // Subscribe FIRST so any events during the snapshot fetch are queued.
  subscription = subscribeEvents({
    onOpen: () => {
      stopPolling();
      void refreshSnapshot();
    },
    onEvent: (event) => {
      applyEvent(event);
    },
    onClose: () => {
      // WS dropped — start polling until WS recovers.
      startPolling();
    },
  });
  void refreshSnapshot();
}

export function stopPeersStore() {
  subscription?.close();
  subscription = null;
  stopPolling();
  state.set(initial);
}

export const peersStore: Readable<PeersState> = { subscribe: state.subscribe };

export const allNodes: Readable<PeerDto[]> = derived(peersStore, ($s) => {
  if (!$s.snapshot) return [];
  return [$s.snapshot.local, ...$s.snapshot.peers].sort(
    (a, b) => b.brain_score - a.brain_score,
  );
});

export const leaderId: Readable<string | null> = derived(
  peersStore,
  ($s) => $s.snapshot?.leader_belief ?? null,
);

// Test-only helper. Not exported via $lib barrel.
export const __test__ = {
  applyEvent,
  state,
};
