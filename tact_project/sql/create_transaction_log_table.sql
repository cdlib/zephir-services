DROP TABLE IF EXISTS transaction_log;

CREATE TABLE IF NOT EXISTS transaction_log (
  id int(11) NOT NULL AUTO_INCREMENT,
  publisher varchar(20) NOT NULL DEFAULT '',
  doi varchar(150) NOT NULL DEFAULT '',
  article_title varchar(500) NOT NULL DEFAULT '',
  corresponding_author varchar(150) NOT NULL DEFAULT '',
  corresponding_author_email varchar(100) NOT NULL DEFAULT '',
  uc_institution varchar(150) NOT NULL DEFAULT '',
  institution_identifier varchar(50) NOT NULL DEFAULT '',
  document_type varchar(50) NOT NULL DEFAULT '',
  eligible varchar(3) NOT NULL DEFAULT '',
  inclusion_date varchar(10) NOT NULL DEFAULT '',
  uc_approval_date varchar(10) NOT NULL DEFAULT '',
  article_access_type varchar(20) NOT NULL DEFAULT '',
  article_license varchar(20) NOT NULL DEFAULT '',
  journal_name varchar(250) NOT NULL DEFAULT '',
  issn_eissn varchar(40) NOT NULL DEFAULT '',
  journal_access_type varchar(20) NOT NULL DEFAULT '',
  journal_subject varchar(50) NOT NULL DEFAULT '',
  grant_participation varchar(3) NOT NULL DEFAULT '',
  funder_information text NOT NULL DEFAULT '',
  full_coverage_reason text NOT NULL DEFAULT '',
  original_apc_usd decimal(9,2) NOT NULL DEFAULT 0.00,
  contractual_apc_usd decimal(9,2) NOT NULL DEFAULT 0.00,
  library_apc_portion_usd decimal(9,2) NOT NULL DEFAULT 0.00,
  author_apc_portion_usd decimal(9,2) NOT NULL DEFAULT 0.00,
  payment_note text NOT NULL DEFAULT '',
  cdl_notes text NOT NULL DEFAULT '',
  license_chosen varchar(100) NOT NULL DEFAULT '',
  journal_bucket varchar(50) NOT NULL DEFAULT '',
  agreement_manager_profile_name varchar(70) NOT NULL DEFAULT '',
  publisher_status varchar(50) NOT NULL DEFAULT '',
  transaction_status varchar(10) NOT NULL DEFAULT '',
  create_date timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_edit timestamp NOT NULL DEFAULT  CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

create index log_doi_index on transaction_log (doi);
