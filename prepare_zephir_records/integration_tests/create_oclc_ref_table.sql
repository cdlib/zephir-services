DROP TABLE IF EXISTS oclc_ref;

CREATE TABLE oclc_ref (
  ocn varchar(16) NOT NULL,
  primary_ocn varchar(16) NOT NULL
);

LOAD DATA LOCAL INFILE 'test_datasets/leveldb_test_datasets.csv'
INTO TABLE oclc_ref 
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
