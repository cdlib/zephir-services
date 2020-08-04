import datetime
import os

import click


@click.command()
@click.argument("input1_path", nargs=1, type=click.Path(exists=True))
@click.argument("input2_path", nargs=1, type=click.Path(exists=True))
@click.argument("output_path", type=click.Path())
@click.pass_context
def concat_files(ctx, input1_path, input2_path, output_path="."):
    """Concatenate two files into one file

    INPUT1_PATH is the path of first file to concat

    INPUT2_PATH is the path of second file to concat

    OUTPUT_PATH is the path of the concatated files
    """

    if os.path.isdir(input1_path):
        raise Exception("A valid path to the validated concordance csv is required")

    if os.path.isdir(input2_path):
        raise Exception("A valid path to the validated concordance csv is required")

    today = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    concat_file = "{}.concatenated.csv".format(today)
    if os.path.exists(output_path) and os.path.isdir(output_path):
        concat_file_path = os.path.join(output_path, concat_file)
    else:
        concat_file_path = output_path

    filenames = [input1_path, input2_path]
    with open(concat_file_path, "w") as outfile:
        for fname in filenames:
            with open(fname) as infile:
                for line in infile:
                    outfile.write(line)
    return concat_file_path


if __name__ == "__main__":
    concat_files()
