import datetime
import os

import click
import pandas as pd


@click.command()
@click.argument("input_path", nargs=1, type=click.Path(exists=True))
@click.argument("output_path", type=click.Path())
@click.pass_context
def create_cluster_file(ctx, input_path, output_path="."):
    """Create a cluster datafile for use in building a
    resolution table for concordance lookup

    INPUT_PATH is the path of a pre-validated concordance tsv file

    OUTPUT_PATH is the path for the generated cluster tsv file
    """

    if os.path.isdir(input_path):
        raise Exception("A valid path to the validated concordance file is required")
    validated_concord_path = input_path
    today = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    cluster_file = "{}.primary-to-multi-cluster.tsv".format(today)
    if os.path.exists(output_path) and os.path.isdir(output_path):
        cluster_file_path = os.path.join(output_path, cluster_file)
    else:
        cluster_file_path = output_path

    # load the validated concordance from HT
    validated_concord_df = pd.read_csv(
        validated_concord_path, sep="\t", names=["oclc", "primary"]
    )
    # sort and export
    validated_concord_df[["primary", "oclc"]].sort_values(["primary", "oclc"]).to_csv(
        cluster_file_path, index=False, header=False, sep="\t"
    )
    return cluster_file_path


if __name__ == "__main__":
    create_cluster_file()
