import os

import click

from lib.export_cache import ExportCache
from lib.utils import ConsoleMessenger


@click.command()
@click.argument("files", nargs=2, required="true")
@click.option(
    "-q", "--quiet", "verbosity", flag_value=0, help="Only emit error messages"
)
@click.option(
    "-v",
    "--verbose",
    "verbosity",
    flag_value=1,
    help="Emit messages dianostic messages",
)
@click.option(
    "-vv",
    "--very-verbose",
    "verbosity",
    flag_value=2,
    help="Emit messages excessive debugging messages",
)
@click.option(
    "-vb", "--verbosity", "verbosity", default=0, help="Set the verbosity of messages"
)
@click.pass_context
def compare_file_cli(ctx, files, verbosity):
    """Compare export files for content differences."""
    console = ConsoleMessenger(app="ZEPHIR-EXPORT", verbosity=verbosity)
    count = 0
    with open(files[0]) as a, open(files[1]) as b:
        for line_a in a:
            count += 1
            if line_a != b.readline():
                console.info("🍎🍊  Differences start on line: {}".format(count))
                raise SystemExit(0)

    console.info("🍎🍎  No differences found between files")


if __name__ == "__main__":
    compare_file_cli()
