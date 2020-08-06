DROP TABLE IF EXISTS cid_minting_store;

CREATE TABLE IF NOT EXISTS cid_minting_store (
        id integer PRIMARY KEY AUTOINCREMENT,
        type char(50) DEFAULT NULL,
        identifier char(255) DEFAULT NULL,
        cid char(11) DEFAULT NULL,
	updated DATETIME DEFAULT CURRENT_TIMESTAMP
); 

