import os

import click

from export_cache import ExportCache
from lib.new_utils import ConsoleMessenger


@click.command()
@click.argument("files", nargs=2, required="true")
@click.option(
    "-q", "--quiet", is_flag=True, default=False, help="Only emit error messages"
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    default=False,
    help="Emit messages dianostic messages",
)
@click.pass_context
def compare_cache_cli(ctx, files, quiet, verbose):
    """Compare export caches for content differences. Ignores datetime of cache creation."""
    console = ConsoleMessenger(quiet, verbose)
    f1_cache = ExportCache(path=set_abs_filepath(files[0]))
    f1_set = f1_cache.frozen_content_set()
    f2_cache = ExportCache(path=set_abs_filepath(files[1]))
    f2_set = f2_cache.frozen_content_set()
    if hash(f1_set) != hash(f2_set):
        for line in f1_set - f2_set:
            console.out("-(cid:{},key:{})".format(line[0], line[1]))
        for line in f2_set - f1_set:
            console.out("+(cid:{},key:{})".format(line[0], line[1]))
        SystemExit(0)
    else:
        console.info("No differences found between cache files")


def set_abs_filepath(file):
    if os.path.isabs(file):
        return file
    else:
        return os.path.join(os.getcwd(), file)


if __name__ == "__main__":
    compare_cache_cli()
