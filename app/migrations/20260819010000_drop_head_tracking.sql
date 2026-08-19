-- 20260819010000_drop_head_tracking.sql
--
-- Head tracking is gone: the client now synchronizes via Automerge's built-in
-- sync protocol, which keeps a per-connection in-memory `sync::State` and
-- renegotiates from scratch on reconnect. The `sync_state` table that persisted
-- last-synced heads is no longer used.

DROP TABLE IF EXISTS sync_state;
