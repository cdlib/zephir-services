DROP TABLE IF EXISTS cid_minting_store;

CREATE TABLE IF NOT EXISTS cid_minting_store (
        id integer PRIMARY KEY AUTOINCREMENT,
        type char(50) DEFAULT NULL,
        identifier char(255) DEFAULT NULL,
        cid char(11) DEFAULT NULL
); 

INSERT INTO cid_minting_store(type, identifier, cid)
VALUES
("oclc", "8727632", "002492721"),
("contrib_sys_id", "pur215476", "002492721"),
("oclc", "32882115", "011323405"),
("contrib_sys_id", "pur864352", "011323405"),
("contrib_sys_id", "uc1234567", "011323405");
