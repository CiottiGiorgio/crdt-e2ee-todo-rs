CREATE TABLE automerge_doc (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    data BLOB NOT NULL
);
