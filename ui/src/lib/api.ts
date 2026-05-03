// Typed API client for harness-api.
//
// Two surfaces: REST (snapshot fetch + status) and WebSocket (live events).
// The WS client subscribes BEFORE issuing the snapshot fetch so events
// that arrive during the fetch are queued and applied after the snapshot
// is in place — see plan §6 race fix.

import type { MeshEvent, PeersSnapshot, StatusDto } from "./types";

const API_BASE = "/api/v1";

export async function fetchPeers(): Promise<PeersSnapshot> {
  const res = await fetch(`${API_BASE}/peers`, {
    headers: { Accept: "application/json" },
  });
  if (!res.ok) throw new Error(`peers fetch ${res.status}`);
  return (await res.json()) as PeersSnapshot;
}

export async function fetchStatus(): Promise<StatusDto> {
  const res = await fetch(`${API_BASE}/status`, {
    headers: { Accept: "application/json" },
  });
  if (!res.ok) throw new Error(`status fetch ${res.status}`);
  return (await res.json()) as StatusDto;
}

export interface EventSubscription {
  /** Cancel the subscription and close the WebSocket. */
  close(): void;
  /** True once the WebSocket OPEN event has fired at least once. */
  isOpen(): boolean;
}

export interface SubscribeOptions {
  onEvent: (event: MeshEvent) => void;
  onOpen?: () => void;
  onClose?: (info: { code: number; reason: string }) => void;
  onError?: (err: Event) => void;
}

/**
 * Subscribe to /api/v1/events with auto-reconnect (capped exponential backoff).
 *
 * The caller owns the snapshot fetch separately — the convention is:
 *   1. start subscription (events go into a queue)
 *   2. await fetchPeers()
 *   3. apply queued events after the snapshot
 * The peers store handles step 3 internally.
 */
export function subscribeEvents(options: SubscribeOptions): EventSubscription {
  let ws: WebSocket | null = null;
  let closed = false;
  let opened = false;
  let attempts = 0;

  function url() {
    const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    return `${proto}//${window.location.host}${API_BASE}/events`;
  }

  function connect() {
    if (closed) return;
    ws = new WebSocket(url());
    ws.onopen = () => {
      attempts = 0;
      opened = true;
      options.onOpen?.();
    };
    ws.onmessage = (msg) => {
      try {
        const event = JSON.parse(msg.data) as MeshEvent;
        options.onEvent(event);
      } catch (err) {
        // Malformed frame — ignore, log to console for debugging.
        // eslint-disable-next-line no-console
        console.warn("harness: bad WS frame", err);
      }
    };
    ws.onerror = (err) => {
      options.onError?.(err);
    };
    ws.onclose = (ev) => {
      options.onClose?.({ code: ev.code, reason: ev.reason });
      ws = null;
      if (closed) return;
      // Backoff: 250ms, 500, 1000, 2000, 4000, capped at 5000 ms; ±20% jitter.
      const base = Math.min(250 * 2 ** attempts, 5000);
      const jitter = base * (0.8 + Math.random() * 0.4);
      attempts += 1;
      setTimeout(connect, jitter);
    };
  }

  connect();

  return {
    close() {
      closed = true;
      ws?.close(1000, "client closing");
    },
    isOpen() {
      return opened && ws?.readyState === WebSocket.OPEN;
    },
  };
}
