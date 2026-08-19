-- 20260819000000_automerge_doc.sql
--
-- The server is now an authoritative automerge peer instead of an opaque delta
-- relay. Drop the seq-id delta log and store the single authoritative document
-- (plaintext structure, ciphertext values) as a single-row BLOB.

DROP VIEW IF EXISTS server_state;
DROP TABLE IF EXISTS deltas;

CREATE TABLE automerge_doc (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    data BLOB NOT NULL
);
