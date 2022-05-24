DROP TABLE IF EXISTS cid_minting_store;

CREATE TABLE IF NOT EXISTS cid_minting_store (
        id integer PRIMARY KEY AUTOINCREMENT,
        type text DEFAULT NULL,
        identifier text DEFAULT NULL,
        cid text DEFAULT NULL,
	updated DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (type,identifier,cid)
);  

