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

**Note**: $ will need to be escaped on the command line.
