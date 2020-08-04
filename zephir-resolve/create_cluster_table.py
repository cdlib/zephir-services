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
def create_cluster_table(ctx, input_path, output_path="."):
    """Create a LevelDB cluster resolution table for concordance lookup

    INPUT_PATH is the path of a generated cluster tsv file

    OUTPUT_PATH is the path for the LevelDB cluster resolution table
    """

    if os.path.isdir(input_path):
        raise Exception("A valid path to the validated concordance tsv is required")
    cluster_file_path = input_path

    if os.path.exists(output_path) and os.path.isdir(output_path):
        db_name = "cluster"
        cluster_db_path = os.path.join(output_path, db_name)
    else:
        cluster_db_path = output_path

    rt = ResolutionTable(os.path.join(cluster_db_path), key=int, value=list)
    with open(cluster_file_path) as f:
        rt.bulk_load(compact_clusters(force_types(csv.reader(f, delimiter="\t"))))
    rt.close()
    return rt.path


def force_types(iter_obj=[]):
    for row in iter_obj:
        yield [int(row[0]), int(row[1])]


def compact_clusters(iter_obj=[]):
    primary_pos = 0
    ocn_pos = 1
    curr_primary = 0
    cluster = []

    for row in iter_obj:
        if row[primary_pos] != curr_primary:
            if curr_primary != 0:
                if len(cluster) > 0:
                    yield [curr_primary, cluster]
            curr_primary = row[primary_pos]
            cluster = []
        if curr_primary != row[ocn_pos]:
            cluster.append(row[ocn_pos])
    if len(cluster) > 0:
        yield [curr_primary, cluster]


if __name__ == "__main__":
    create_cluster_table()
