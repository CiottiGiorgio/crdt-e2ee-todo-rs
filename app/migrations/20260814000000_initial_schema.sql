CREATE TABLE automerge_doc (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    data BLOB NOT NULL
);

CREATE TABLE sync_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    highest_observed INTEGER NOT NULL,
    missing_ids TEXT NOT NULL
);
