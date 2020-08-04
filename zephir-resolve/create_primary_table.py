import csv
import datetime
import os

import click
import plyvel

from resolution_table import ResolutionTable


@click.command()
@click.argument("input_path", nargs=1, type=click.Path(exists=True))
@click.argument("output_path", type=click.Path())
@click.pass_context
def create_primary_table(ctx, input_path, output_path="."):
    """Create a LevelDB primary resolution table for concordance lookup

    INPUT_PATH is the path of a generated primary tsv file

    OUTPUT_PATH is the path for the LevelDB primary resolution table
    """

    if os.path.isdir(input_path):
        raise Exception("A valid path to the primary tsv is required")
    primary_file_path = input_path

    if os.path.exists(output_path) and os.path.isdir(output_path):
        db_name = "primary-table"
        primary_db_path = os.path.join(output_path, db_name)
    else:
        primary_db_path = output_path

    rt = ResolutionTable(os.path.join(primary_db_path), key=int, value=int)
    with open(primary_file_path) as f:
        rt.bulk_load(force_types(csv.reader(f, delimiter="\t")))
    rt.close()
    return rt.path


def force_types(iter_obj=[]):
    for row in iter_obj:
        yield [int(row[0]), int(row[1])]


if __name__ == "__main__":
    create_primary_table()
