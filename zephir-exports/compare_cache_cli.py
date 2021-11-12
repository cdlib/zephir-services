import os

import click

from lib.export_cache import ExportCache
import lib.utils as utils

root_dir = os.path.join(os.path.dirname(__file__))
exec(
    compile(
        source=open(os.path.join(os.path.dirname(__file__), "shared_cli.py")).read(),
        filename="shared_cli.py",
        mode="exec",
    )
)


@click.command("compare-cache")
@pass_state
@common_verbose_options
@click.argument("files", nargs=2, required="true")
def compare_cache_cli(state, files, **kwargs):
    """Compare export caches for content differences. Ignores datetime of cache creation."""
    try:
        app = utils.application_setup(
            root_dir=os.path.join(os.path.dirname(__file__)), state=state, kwargs=kwargs
        )
        return_code = compare_cache_cmd(app, files)
    except Exception as err:
        return_code = 1
        raise click.ClickException(err)
    click.Context.exit(return_code)


def compare_cache_cmd(app, files):
    f1_cache = ExportCache(path=set_abs_filepath(files[0]))
    f1_set = f1_cache.frozen_content_set()
    f2_cache = ExportCache(path=set_abs_filepath(files[1]))
    f2_set = f2_cache.frozen_content_set()
    if hash(f1_set) != hash(f2_set):
        for line in f1_set - f2_set:
            app.console.out("-(cid:{},key:{})".format(line[0], line[1]))
        for line in f2_set - f1_set:
            app.console.out("+(cid:{},key:{})".format(line[0], line[1]))
        app.console.info("Differences found between cache files")
    else:
        app.console.info("No differences found between cache files")
    return 0


def set_abs_filepath(file):
    if os.path.isabs(file):
        return file
    else:
        return os.path.join(os.getcwd(), file)


if __name__ == "__main__":
    compare_cache_cli()
