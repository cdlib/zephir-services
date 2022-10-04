DROP TABLE IF EXISTS cid_minting_store;

CREATE TABLE IF NOT EXISTS cid_minting_store (
        id integer PRIMARY KEY AUTOINCREMENT,
        type text DEFAULT NULL,
        identifier text DEFAULT NULL,
        cid text DEFAULT NULL,
	updated DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (type,identifier)
);

INSERT INTO cid_minting_store(type, identifier, cid)
VALUES
("ocn", "8727632", "002492721"),
("ocn", "87276322", "002492722"),
("sysid", "pur215476", "002492721"),
("sysid", "pur.215476", "102492721"),
("sysid", "pur1234567", "002492727"),
("ocn", "32882115", "011323405"),
("sysid", "pur864352", "011323405"),
("sysid", "uc1234567", "011323405");
