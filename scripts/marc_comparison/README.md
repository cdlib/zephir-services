# MARC Comparison

This is a collection of scripts used for processing and comparing MARC records.

## Field builder (field_builder.py)
The field builder script allows quick modification of a MARC file by supplying it with a field building pattern.

### Usage
The field builder script simply takes an input file, output file, and a pattern, which is used to update (or add) a field for each record in a file.

The pattern must always follow the format LOCATION=EXPRESSION where the LOCATION is the MARC location you would like to change or create (e.g., 001, 245$a, 264$b), and EXPRESSION is any mixture of plain text and MARC information you'd like to incorporate, surrounded by {}.

For example, if you want to construct a 974$u out of a 001 and the prefix hvd, you would supply the script with 974$u=hvd.{001} to change (or create) a 974$u containing the 001 field prefixed with hvd. This is how this would be run on the command line:

```
pipenv run python field_builder.py --input input_file.xml --output output_file.xml --pattern 974\$u=hvd.{001}
```

## Printout Script (id_printout.py)
The printout script allows for the extraction of a specific field from a MARC file.

### Usage
The printout script takes an input file and a field to extract from the input file, and prints out the values of that field for each record in the file.

```
pipenv run python id_printout.py --file input_file.xml --idloc 974\$u
```

## Export MARC Item Records (export_marc_item_records.py)
This script reads identifiers from a file, queries the database for corresponding MARC records, and writes the MARCXML output to records.xml. If the db.yml is not found, it will prompt for MySQL connection details.

## Usage

```bash
python export_marc_item_records.py -f <input_file> -o <output_file> [--db_config_path <db_config.yml>] [--db_env <environment>]
```

### Options
```yaml
-f, --file: Path to the input file containing item IDs. Each ID should be on a separate line.
-o, --output: Path to the output XML file for the MARCXML records.
--db_config_path: Optional. Path to the database configuration file in YAML format. If not provided and the default db.yml is not found, the script will prompt for MySQL connection details.
--db_env: Optional. Specifies the environment section within the database configuration file to use. Default: production
Configuration File Format

-h: Show the usage information.
```

### Example
Running the script with an input file ids.txt and outputting the MARCXML records to records.xml:

```bash
python export_marc_item_records.py -f ids.txt -o records.xml
```

### Database Configuration File
The database configuration file is option. If none is specified, the user will be asked for connection details to a MySQL server. Database configurations, if given, should be in YAML format. Here's an example structure for db.yml (default location):

```yaml
test: 
    drivername: 'sqlite'
    database: 'test_database.sqlite'
production:
    type: 'mysql'
    host: 'localhost'
    database: 'htmm'
    port: 3306
    user: 'your_username'
    password: 'your_password'
```

### Input File Format
The input file should contain one identifier per line. Here is an example of how the input file should look:

```plaintext
htid.12345
miu.67890
uc1.24680
```

## Match Up File Script (match_up_file.py)
The script will compare the records and output a summary of the differences, which includes the count of differences in these fields:
- LDR/06 & 07			
- HOL$1 (bib ID no.)	
- 008/07-10 (Date1)		
- 008/11-14 (Date2)		
- 008/15-17 (Pub Place)	
- 008/28 (govdoc code f)	
- 008/17 & 28 (for US feddoc)	
- OCLC number
- 260$a (place of pub)
- 260$b (name of pub)
- 260$c (date of pub)
- 264$a (place of pub)
- 264$b (name of pub)
- 264$c (date of pub)

### Usage
The match up file script takes two files and two id location fields and matches them up.

```
pipenv run python match_up_file.py --file1 file1.xml --file2 file2.xml --idloc1 974\$u --idloc2 974\$u
```

**Note**: $ will need to be escaped on the command line, for example "974\$u".
