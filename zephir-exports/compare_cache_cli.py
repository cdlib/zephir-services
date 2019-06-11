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
def compare_cache_cli(ctx, files, verbosity):
    """Compare export caches for content differences. Ignores datetime of cache creation."""
    console = ConsoleMessenger(app="ZEPHIR-EXPORT", verbosity=verbosity)
    f1_cache = ExportCache(path=set_abs_filepath(files[0]))
    f1_set = f1_cache.frozen_content_set()
    f2_cache = ExportCache(path=set_abs_filepath(files[1]))
    f2_set = f2_cache.frozen_content_set()
    if hash(f1_set) != hash(f2_set):
        for line in f1_set - f2_set:
            console.out("-(cid:{},key:{})".format(line[0], line[1]))
        for line in f2_set - f1_set:
            console.out("+(cid:{},key:{})".format(line[0], line[1]))
        console.info("Differences found between cache files  🍎 ==🍊")
    else:
        console.info("No differences found between cache files  🍎 ==🍎")


def set_abs_filepath(file):
    if os.path.isabs(file):
        return file
    else:
        return os.path.join(os.getcwd(), file)


if __name__ == "__main__":
    compare_cache_cli()
