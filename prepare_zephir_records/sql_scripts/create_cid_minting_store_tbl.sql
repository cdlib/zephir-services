DROP TABLE IF EXISTS cid_minting_store;

CREATE TABLE cid_minting_store (
  id int(10) unsigned NOT NULL AUTO_INCREMENT,
  type varchar(50) DEFAULT NULL,
  identifier varchar(255) DEFAULT NULL,
  cid varchar(11) DEFAULT NULL,
  updated timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY unique_cid_minting_store (type,identifier),
  KEY index_cid_minting_store_cid (cid)
);


