import click

from ht_bib_cache import generate_cache
from ht_bib_export_full import generate_export_full
from ht_bib_export_incr import generate_export_incr

@click.command()
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
@click.option("--selection", nargs=1, required="true")
@click.option("--export-type", nargs=1, required="true")
@click.option("--cache-only", nargs=1)
@click.option("--use-cache", nargs=1)
@click.option(
    "-f",
    "--force",
    is_flag=True,
    default=False,
    help="Remove and rewrite over existing cache",
)
def ht_bib_cli(quiet, verbose, selection, export_type, cache_only, use_cache, force):
    cache = use_cache or generate_cache(selection, quiet, verbose, force)
    output = None
    if export_type == "full":
        output = generate_export_full(selection=selection, use_cache=cache, quiet=quiet, verbose=verbose, force=force)
    elif export_type == "full":
        output = generate_export_incre(selection=selection, use_cache=cache, quiet=quiet, verbose=verbose, force=force)

if __name__ == "__main__":
    ht_biblio_cli()
