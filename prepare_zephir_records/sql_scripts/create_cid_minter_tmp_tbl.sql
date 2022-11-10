DROP TABLE IF EXISTS cid_minter_tmp;

CREATE TABLE cid_minter_tmp LIKE cid_minter;

INSERT INTO cid_minter_tmp SELECT * FROM cid_minter;
