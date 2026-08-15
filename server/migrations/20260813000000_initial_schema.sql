-- 20260813000000_initial_schema.sql

CREATE TABLE deltas (
    seq_id INTEGER PRIMARY KEY,
    ciphertext BLOB NOT NULL,
    nonce BLOB NOT NULL CHECK (length(nonce) = 12)
);

CREATE VIEW server_state AS
SELECT 
    COALESCE(MAX(seq_id), 0) AS highest_seq_id
FROM deltas;
