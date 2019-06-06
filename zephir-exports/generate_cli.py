import click

from export_types.ht_bib_cache import ht_bib_cache
from export_types.ht_bib_full import ht_bib_full
from export_types.ht_bib_incr import ht_bib_incr
from lib.new_utils import ConsoleMessenger


@click.command()
@click.argument("export-type")
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
@click.option(
    "-vv",
    "--very-verbose",
    is_flag=True,
    default=False,
    help="Emit messages excessive debugging messages",
)
@click.option(
    "-mv",
    "--merge-version",
    nargs=1,
    required="yes",
    help="Specify the merge algoithm version for biblographic creation",
)
@click.option(
    "-f",
    "--force",
    is_flag=True,
    default=False,
    help="Remove and rewrite over existing cache",
)
@click.pass_context
def generate_cli(ctx, export_type, quiet, verbose, very_verbose, merge_version, force):
    """Generate Zephir exports files for HathiTrust."""
    console = ConsoleMessenger(quiet, verbose, very_verbose)
    cache = ht_bib_cache(
        console=console,
        merge_version=merge_version,
        quiet=quiet,
        verbose=verbose,
        very_verbose=very_verbose,
        force=force,
    )
    if export_type == "ht-bib-full":
        ht_bib_full(
            console=console,
            merge_version=merge_version,
            quiet=quiet,
            verbose=verbose,
            very_verbose=very_verbose,
            force=force,
        )
    elif export_type == "ht-bib-incr":
        ht_bib_incr(
            console=console,
            merge_version=merge_version,
            quiet=quiet,
            verbose=verbose,
            very_verbose=very_verbose,
            force=force,
        )


if __name__ == "__main__":
    generate()
