import click

from exports.ht_bib_cache import ht_bib_cache
from exports.ht_bib_full import ht_bib_full
from exports.ht_bib_incr import ht_bib_incr
from lib.utils import ConsoleMessenger


@click.command()
@click.argument("export-type")
@click.option(
    "-q", "--quiet", "verbosity", flag_value=1, help="Only emit error messages"
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
def generate_cli(ctx, export_type, verbosity, merge_version, force):
    """Generate Zephir exports files for HathiTrust."""
    console = ConsoleMessenger(app="ZEPHIR-EXPORT", verbosity=verbosity)
    ht_bib_cache(console=console, merge_version=merge_version, force=force)
    if export_type == "ht-bib-full":
        ht_bib_full(console=console, merge_version=merge_version, force=force)
    elif export_type == "ht-bib-incr":
        ht_bib_incr(console=console, merge_version=merge_version, force=force)


if __name__ == "__main__":
    generate_cli()
