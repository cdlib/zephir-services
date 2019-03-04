DROP DATABASE IF EXISTS htmm;
CREATE DATABASE htmm;
USE htmm;

DROP TABLE IF EXISTS `zephir_identifier_records`;

CREATE TABLE `zephir_identifier_records` (
  `record_autoid` int(11) NOT NULL,
  `identifier_autoid` int(11) NOT NULL,
  PRIMARY KEY (`record_autoid`,`identifier_autoid`),
  KEY `index_zephir_identifier_records_identifier_autoid` (`identifier_autoid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;



# Dump of table zephir_identifiers
# ------------------------------------------------------------

DROP TABLE IF EXISTS `zephir_identifiers`;

CREATE TABLE `zephir_identifiers` (
  `autoid` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `type` varchar(50) DEFAULT NULL,
  `identifier` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`autoid`),
  UNIQUE KEY `unique_zephir_identifiers_id_type` (`type`,`identifier`),
  KEY `index_zephir_identifiers_identifier` (`identifier`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;



# Dump of table zephir_records
# ------------------------------------------------------------

DROP TABLE IF EXISTS `zephir_records`;

CREATE TABLE `zephir_records` (
  `autoid` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `cid` varchar(11) DEFAULT NULL,
  `contribsys` varchar(25) DEFAULT NULL,
  `contribsys_id` varchar(100) DEFAULT NULL,
  `creator` varchar(500) DEFAULT NULL,
  `creator_date` varchar(100) DEFAULT NULL,
  `core_version` varchar(8) DEFAULT NULL,
  `dig_agent` varchar(20) DEFAULT NULL,
  `enumchron` varchar(250) DEFAULT NULL,
  `id` varchar(38) DEFAULT NULL,
  `language` varchar(3) DEFAULT NULL,
  `last_updated_at` datetime DEFAULT NULL,
  `loader_version` varchar(20) DEFAULT NULL,
  `namespace` varchar(5) DEFAULT NULL,
  `original_format` varchar(2) DEFAULT NULL,
  `process_date` varchar(20) DEFAULT NULL,
  `publication_date` varchar(4) DEFAULT NULL,
  `publication_place` varchar(3) DEFAULT NULL,
  `publisher` varchar(1000) DEFAULT NULL,
  `resource_type` varchar(25) DEFAULT NULL,
  `scan_id` varchar(120) DEFAULT NULL,
  `source` varchar(20) DEFAULT NULL,
  `source_collection` varchar(10) DEFAULT NULL,
  `source_record_number` varchar(100) DEFAULT NULL,
  `title` varchar(3000) DEFAULT NULL,
  `identifiers_json` text,
  `metadata_namespace` varchar(40) DEFAULT NULL,
  `version_date` datetime DEFAULT NULL,
  `version` varchar(50) DEFAULT NULL,
  `set_id` varchar(100) DEFAULT NULL,
  `set_method` varchar(20) DEFAULT NULL,
  `set_source` varchar(255) DEFAULT NULL,
  `metadata_date` datetime DEFAULT NULL,
  `metadata_file_date` datetime DEFAULT NULL,
  `stylesheet_date` datetime DEFAULT NULL,
  `shadow_date` datetime DEFAULT NULL,
  `overlay_date` datetime DEFAULT NULL,
  `attr_ingest_date` datetime DEFAULT NULL,
  `attr_rights_flag` datetime DEFAULT NULL,
  `attr_cid_flag` datetime DEFAULT NULL,
  `var_score` int(11) DEFAULT NULL,
  `var_usfeddoc` tinyint(1) DEFAULT NULL,
  `metadata_filekey` varchar(40) DEFAULT NULL,
  `core_filekey` varchar(40) DEFAULT NULL,
  `attributes_filekey` varchar(40) DEFAULT NULL,
  `db_updated_at` datetime DEFAULT NULL,
  PRIMARY KEY (`autoid`),
  UNIQUE KEY `unique_zephir_records_id` (`id`),
  KEY `index_zephir_records_cid` (`cid`),
  KEY `index_zephir_records_namespace` (`namespace`),
  KEY `index_zephir_records_original_format` (`original_format`),
  KEY `index_zephir_records_publication_date` (`publication_date`),
  KEY `index_zephir_records_resource_type` (`resource_type`),
  KEY `index_zephir_records_scan_id` (`scan_id`),
  KEY `index_zephir_records_source` (`source`),
  KEY `index_zephir_records_source_collection` (`source_collection`),
  KEY `index_zephir_records_source_record_number` (`source_record_number`),
  KEY `index_zephir_records_metadata_filekey` (`metadata_filekey`),
  KEY `index_zephir_records_core_filekey` (`core_filekey`),
  KEY `index_zephir_records_attributes_filekey` (`attributes_filekey`),
  KEY `index_zephir_records_var_usfeddoc` (`var_usfeddoc`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;
