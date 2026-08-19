-- 20260819000000_head_tracking.sql
--
-- Replace the seq-id based sync bookkeeping with last-synced head tracking.
-- The client persists the automerge heads it has already synchronized with the
-- server as a comma-separated list of hex-encoded change hashes.

DROP TABLE IF EXISTS sync_state;

CREATE TABLE sync_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_synced_heads TEXT NOT NULL
);
