[![Build Status](https://travis-ci.org/cdlib/zephir-services.svg?branch=master)](https://travis-ci.org/cdlib/zephir-services)
[![codecov](https://codecov.io/gh/cdlib/zephir-services/branch/master/graph/badge.svg)](https://codecov.io/gh/cdlib/zephir-services)

# Zephir services

Zephir services is a collection of Python programs and scripts for the Zephir
project. The projects share a common Pipenv environment, but each subfolder
provides a distinct service for Zephir. See README within subfolders for
more information.

## Find records by barcode

`scripts/find_records_with_barcodes.py` extracts the MARC records whose 955$b
barcodes appear in a text file containing one barcode per line. Pass the MARC
XML files in the order they should be searched. If a barcode occurs more than
once, the record in the last supplied file containing it is written to the
output.

Run the script from the repository root with Pipenv:

```bash
pipenv run python scripts/find_records_with_barcodes.py \
    --xml-file-list path_list.txt \
    --barcodes-file barcodes.txt \
    > records.xml \
    2> errors.txt
```

If you would like to pass either the XML paths or the barcodes as standard input, you can use `-` and pipe in the data. You cannot have both use stdin. For example if you have a directory called `xml_files`:
```bash
ls -1 xml_files/*.xml | \
    pipenv run python scripts/find_records_with_barcodes.py \
    --xml-file-list - \
    --barcodes-file barcodes.txt \
    > records.xml \
    2> errors.txt
```

The output is a MARC XML collection. XML files that cannot be parsed and
records whose barcode cannot be read are skipped and reported to standard error.
