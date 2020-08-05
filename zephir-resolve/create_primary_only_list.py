import datetime
import os

import click
import pandas as pd


@click.command()
@click.argument("input_path", nargs=1, type=click.Path(exists=True))
@click.argument("output_path", type=click.Path())
@click.pass_context
def create_primary_only_list(ctx, input_path, output_path="."):
    """Create a cluster datafile for use in building a
    resolution table for concordance lookup

    INPUT_PATH is the path of a concordance file

    OUTPUT_PATH is the path for primary only list tsv file
    """

    if os.path.isdir(input_path):
        raise Exception("A valid path to the concordance file is required")
    raw_concord_path = input_path

    today = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    primary_only_file = "{}.primary_only_list.tsv".format(today)
    if os.path.exists(output_path) and os.path.isdir(output_path):
        primary_only_file_path = os.path.join(output_path, primary_only_file)
    else:
        primary_only_file_path = output_path

    # load the raw concordance from OCLC
    raw_concord_df = pd.read_csv(raw_concord_path, sep="\t", names=["oclc", "primary"])

    # create dataframe of only the master oclc numbers
    primary_only_df = raw_concord_df[
        raw_concord_df["oclc"] == raw_concord_df["primary"]
    ].copy()

    # sort and export
    primary_only_df[["oclc", "primary"]].sort_values("oclc").to_csv(
        primary_only_file_path, index=False, header=False, sep="\t"
    )
    return primary_only_file_path


if __name__ == "__main__":
    create_primary_only_list()
