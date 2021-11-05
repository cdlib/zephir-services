import os

import click

from exports.ht_bib_cache import ht_bib_cache
from exports.ht_bib_full import ht_bib_full
from exports.ht_bib_incr import ht_bib_incr
import lib.utils as utils

# load and run block for shared cli functions, like verbosity
root_dir = os.path.join(os.path.dirname(__file__))
exec(
    compile(
        source=open(os.path.join(os.path.dirname(__file__), "shared_cli.py")).read(),
        filename="shared_cli.py",
        mode="exec",
    )
)

# create command name
@click.command("generate")
# pass state variable used in verbose options
@pass_state
# include verbose options defined in shared_cli
@common_verbose_options
# include arguments here
@click.argument("export-type")
@click.option(
    "-mv",
    "--merge-version",
    nargs=1,
    required="yes",
    help="Specify the merge algoithm version for biblographic creation",
)
@click.option(
    "-o",
    "--output-path",
    nargs=1,
    help="File or directory path for the generated export",
)
@click.option(
    "-c", "--cache-path", nargs=1, help="File or directory path for export cache"
)
@click.option(
    "-f",
    "--force",
    is_flag=True,
    default=False,
    help="Remove and rewrite over existing cache",
)
def generate_cli(state, **kwargs):
    """Generate Zephir exports files for HathiTrust."""

    try:
        # setup an application given state and kwargs
        app = utils.application_setup(
            root_dir=os.path.join(os.path.dirname(__file__)), state=state, kwargs=kwargs
        )
        # run command with application context, and argument
        return_code = generate_cmd(app)
    except Exception as err:
        return_code = 1
        raise click.ClickException(err)


def generate_cmd(app):
    if app.args["cache_path"] and os.path.exists(app.args["cache_path"]):
        app.console.debug("Using existing cache {}".format(app.args["cache_path"]))
        cache = app.args["cache_path"]
    else:
        cache = ht_bib_cache(
            console=app.console,
            cache_path=app.args["cache_path"],
            merge_version=app.args["merge_version"],
            force=app.args["force"],
        )
    if app.args["export_type"] == "ht-bib-full":
        ht_bib_full(
            console=app.console,
            cache_path=cache,
            output_path=app.args["output_path"],
            merge_version=app.args["merge_version"],
            force=app.args["force"],
        )
    elif app.args["export_type"] == "ht-bib-incr":
        ht_bib_incr(
            console=app.console,
            cache_path=cache,
            output_path=app.args["output_path"],
            merge_version=app.args["merge_version"],
            force=app.args["force"],
        )


if __name__ == "__main__":
    generate_cli()
