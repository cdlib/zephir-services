import click

from export_cache import ExportCache


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
    help="Emit messages dianostic messages about everything",
)
def compare_cache_cli(files, quiet, verbose):
    f1_cache = ExportCache(path=files[0])
    f2_cache = ExportCache(path=files[1])
    if (
        f1_cache.size() == f2_cache.size()
        and f1_cache.content_hash() == f2_cache.content_hash()
    ):
        print("Success: The cache calculatations are identical")
        SystemExit(0)
    else:
        print("Failure: The cache calculatations are not identical")
        SystemExit(2)


if __name__ == "__main__":
    compare_cache_cli()
