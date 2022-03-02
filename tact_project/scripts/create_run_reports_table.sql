DROP TABLE IF EXISTS run_reports;

CREATE TABLE IF NOT EXISTS run_reports (
  id int(11) NOT NULL AUTO_INCREMENT,
  publisher char(20) GENERATED ALWAYS AS (json_unquote(json_extract(run_report,'$.publisher'))) VIRTUAL,
  filename char(100) GENERATED ALWAYS AS (json_unquote(json_extract(run_report,'$.filename'))) VIRTUAL,
  run_datetime char(30) GENERATED ALWAYS AS (json_unquote(json_extract(run_report,'$.run_datetime'))) VIRTUAL,
  input_records int GENERATED ALWAYS AS (json_unquote(json_extract(run_report,'$.input_records'))) VIRTUAL,
  total_processed_records int GENERATED ALWAYS AS (json_unquote(json_extract(run_report,'$.total_processed_records'))) VIRTUAL,
  rejected_records int GENERATED ALWAYS AS (json_unquote(json_extract(run_report,'$.rejected_records'))) VIRTUAL,
  new_records_added int GENERATED ALWAYS AS (json_unquote(json_extract(run_report,'$.new_records_added'))) VIRTUAL,
  existing_records_updated int GENERATED ALWAYS AS (json_unquote(json_extract(run_report,'$.existing_records_updated'))) VIRTUAL,
  status char(1) GENERATED ALWAYS AS (json_unquote(json_extract(run_report,'$.status'))) VIRTUAL,
  run_report json DEFAULT NULL,
  create_date timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_edit timestamp NOT NULL DEFAULT  CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

