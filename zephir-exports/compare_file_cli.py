import click
import os

import lib.utils as utils

root_dir = os.path.join(os.path.dirname(__file__))
exec(
    compile(
        source=open(os.path.join(os.path.dirname(__file__), "shared_cli.py")).read(),
        filename="shared_cli.py",
        mode="exec",
    )
)


@click.command("compare-files")
@pass_state
@common_verbose_options
@click.argument("files", nargs=2, required="true")
def compare_file_cli(state, files, **kwargs):
    """Compare export files for content differences."""
    try:
        app = utils.application_setup(
            root_dir=os.path.join(os.path.dirname(__file__)), state=state, kwargs=kwargs
        )
        return_code = compare_file_cmd(app, files)
    except Exception as err:
        return_code = 1
        raise click.ClickException(err)
    click.Context.exit(return_code)


def compare_file_cmd(app, files):
    count = 0
    with open(files[0]) as a, open(files[1]) as b:
        for line_a in a:
            count += 1
            if line_a != b.readline():
                app.console.info("Differences start on line: {}".format(count))
                raise SystemExit(0)

    app.console.info("No differences found between files")


if __name__ == "__main__":
    compare_file_cli()
