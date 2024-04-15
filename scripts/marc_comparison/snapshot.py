import argparse
import getpass
import io
import os
import sys
import json
from collections import OrderedDict

import yaml
import pandas as pd
from pymarc import XMLWriter, parse_xml_to_array
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker

# Define a dictionary to map 'type' to 'drivername'
DATABASE_DRIVER_MAP = {
    'sqlite': 'sqlite',
    'mysql': 'mysql+mysqlconnector',  # Example for MySQL using mysqlconnector
}

DEFAULT_DB_CONFIG_FILE = "db.yml"
DEFAULT_DB_ENV = "production"

def prompt_for_mysql_details():
    print("No database configuration found. Please enter MySQL connection details.")
    db_config = {
        'type': 'mysql',
        'host': input("Enter MySQL host: "),
        'user': input("Enter MySQL username: "),
        'password': getpass.getpass("Enter MySQL password: "),
        'port': input("Enter MySQL port (default 3306): ") or 3306,
        'database': input("Enter MySQL database name: "),
    }
    return db_config

def initialize_database_session(db_config_path, db_env=DEFAULT_DB_ENV):
    db_config = None

    # If no config, look for default config. If not, prompt for MySQL details
    if not db_config_path and not os.path.exists(DEFAULT_DB_CONFIG_FILE):
            db_config = prompt_for_mysql_details()
            return db_config
    else:
        db_config_path = db_config_path or DEFAULT_DB_CONFIG_FILE

        # Load the database configuration from a YAML file
        try:
            with open(db_config_path, "r") as file:
                config = yaml.safe_load(file)
                db_config = config.get(db_env)
        except FileNotFoundError:
            print(f"Error: The file {db_config_path} was not found.", file=sys.stderr)
            sys.exit(1)
        except yaml.YAMLError as e:
            print(f"Error parsing YAML file: {e}", file=sys.stderr)
            sys.exit(1)
        except KeyError:
                print(
                f"Error: The environment '{db_env}' was not found in the database configuration file.",
                file=sys.stderr,
            )
                sys.exit(1)

    # Retrieve the database configuration for the specified environment
    # Use the 'type' field from db_config to get the 'drivername'
    db_type = db_config.get('type')
    drivername = DATABASE_DRIVER_MAP.get(db_type)
    if not drivername:
        print(f"Error: Unsupported database type '{db_type}'.", file=sys.stderr)
        sys.exit(1)

    # Construct the database URL
    if db_config["type"] == "sqlite":
        # For SQLite, prepend the absolute path if the database path is not absolute
        database = db_config["database"]
        if not os.path.isabs(database):
            database = os.path.join(os.path.dirname(db_config_path), database)
        url = str(URL.create(drivername, database=database))
    elif db_config["type"] in "mysql":
        url = URL.create(
                drivername,
                username=db_config.get("user"),
                password=db_config.get("password"),
                host=db_config.get("host"),
                port=db_config.get("port"),
                database=db_config.get("database"),
            )
    else:
        print(
            f"Error: The drivername '{db_config['drivername']}' is not supported.",
            file=sys.stderr,
        )
        sys.exit(1)
    print(url)
    try:
        print("Connecting to the database...", file=sys.stderr)
        # Create the SQLAlchemy engine using parameters from the database configuration, not the url
        engine = create_engine(url)

        # Create a SessionMaker
        Session = sessionmaker(bind=engine)
    except (
        Exception
    ) as e:  # It's better to catch specific exceptions related to SQLAlchemy
        print(f"Database connection error: {e}", file=sys.stderr)
        sys.exit(1)

    # Return a session instance
    return Session()


def query_record_information_from_db(session, ids):
    zephir_records = []
    missing_ids = list(ids)  # Start with all IDs as potentially missing

    # SQL databases have limits on the number of items in the IN clause, so chunk the IDs
    chunk_size = 500  # Adjust based on your database's performance and limits
    chunks = [ids[i:i + chunk_size] for i in range(0, len(ids), chunk_size)]

    print("Querying records...", file=sys.stderr)

    for chunk in chunks:
        # Generate the placeholders for each chunk
        placeholders = ','.join([':id{}'.format(i) for i in range(len(chunk))])
        
        # Construct the query with dynamic placeholders
        query_str = f"""
        SELECT zr.id as htid, zr.cid, zr.namespace, zr.source_record_number, attr.name as attr_name, attr.dscr as attr_description, reasons.name as reason_name, reasons.dscr as reason_description, zr.identifiers_json
        FROM zephir_records as zr LEFT JOIN rights_current as rc ON zr.id = rc.volume_identifier
        LEFT JOIN attributes as attr ON rc.attr = attr.id
        LEFT JOIN reasons ON rc.reason = reasons.id
        WHERE zr.id IN ({placeholders})
        """

        # Prepare parameters as a dictionary {placeholder_name: value}
        params = {f'id{i}': chunk[i] for i in range(len(chunk))}

        result = session.execute(text(query_str), params).fetchall()

        found_ids = set(row[0] for row in result)  # Collect found IDs for comparison
        if result:
            for row in result:
                id, cid, namespace, source_record_number, attr_name, attr_dscr, reason_name, reason_dscr, identifiers_json = row
                oclcs = get_oclcs_from_json(identifiers_json)

                ordered_dict = OrderedDict()
                ordered_dict["id"] = id
                ordered_dict["cid"] = cid
                ordered_dict["oclcs"] = ",".join(oclcs)
                ordered_dict["namespace"] = namespace
                ordered_dict["source_record_number"] = source_record_number
                ordered_dict["attr_name"] = attr_name
                ordered_dict["attr_description"] = attr_dscr
                ordered_dict["reason_name"] = reason_name
                ordered_dict["reason_description"] = reason_dscr
                ordered_dict["identifiers_json"] = identifiers_json

                zephir_records.append(ordered_dict)
        
        # Update missing_ids by removing found_ids
        missing_ids = [id for id in missing_ids if id not in found_ids]
    
    if missing_ids:
        print(
            f"Warning: IDs not found in the database: {', '.join(missing_ids)}",
            file=sys.stderr,
        )
        
    return zephir_records

def get_oclcs_from_json(json_str):
    try:
        oclcs = []
        json_list = json.loads(json_str)
        for item in json_list:
            if item["type"] == "oclc":
                oclcs.append(item["identifier"])
        return oclcs
    except json.JSONDecodeError:
        return []

def write_to_csv(records, output_file):
    try:
        print("Writing to file...", file=sys.stderr)
        pd.DataFrame.from_records(records).to_csv(output_file, index=False, sep="\t")
    except FileNotFoundError:
        print(f"Error: The file {output_file} was not found.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred while writing to file: {e}", file=sys.stderr)
        sys.exit(1)

def process_and_generate_snapshot(input_file, output_file, db_config_path, db_env):
    # Load IDs from a file
    try:
        with open(input_file, "r") as f:
            ids = [line.strip() for line in f.readlines()]
    except FileNotFoundError:
        print(f"Error: The file {input_file} was not found.", file=sys.stderr)
        return sys.exit(1)

    # Get a database session
    session = initialize_database_session(db_config_path, db_env)

    # query MARC records
    zephir_records = query_record_information_from_db(session, ids)

    # Convert records to MARC collection and save to output file
    write_to_csv(zephir_records, output_file)

    # print success message with how many records were exported
    print(f"Finished. {len(zephir_records)} records exported",  file=sys.stderr)

    return 0


def main():
    try:
        parser = argparse.ArgumentParser(
            description="""
        This script snapshot data to track cid and rights changes.
        """,
            formatter_class=argparse.RawTextHelpFormatter,
        )  # Use RawTextHelpFormatter for better formatting

        parser.add_argument(
            "-f",
            "--file",
            required=True,
            help="""
                            Path to the input file containing item IDs.
                            Each ID should be on a separate line.
                            Example:
                            htid.12345
                            miu.67890
                            uc1.24680
                            """,
        )

        parser.add_argument(
            "-o",
            "--output",
            required=True,
            help="""
                            Path to the output TSV file.
                            Example: output.tsv
                            """,
        )

        parser.add_argument(
            "--db_config_path",
            help="""
                            Path to the database configuration file in YAML format.
                            This file should contain the database credentials and connection details.
                            Default: 'db.yml'
                            Example file structure:
                            test: 
                                type: 'sqlite'
                                database: 'test_database.sqlite'
                            production:
                                type: 'mysql+mysqlconnector'
                                host: 'localhost'
                                database: 'htmm'
                                port: 3306
                                user: 'your_username'
                                password: 'your_password'
                            """,
        )

        parser.add_argument(
            "--db_env",
            default="production",
            help="""
                            This is only needed when using configuration files for the database. 
                            The environment section within the database configuration file to use.
                            This allows you to define multiple configurations within the same file and select one at runtime.
                            Default: 'production'
                            """,
        )

        args = parser.parse_args()

        return_code = process_and_generate_snapshot(
            args.file, args.output, args.db_config_path, args.db_env
        )
        sys.exit(return_code)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()


