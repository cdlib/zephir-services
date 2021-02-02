from datetime import datetime
import os
import glob

import click

from cids_repo_status import cids_repo_status_cmd
import lib.utils_refactor as utils


# COMMON OPTION BLOCK
# define options to handle verbosity in reusable decorators
class State(object):
    def __init__(self):
        self.verbosity = 0


pass_state = click.make_pass_decorator(State, ensure=True)


def verbose_option(f):
    def callback(ctx, param, value):
        state = ctx.ensure_object(State)
        if value:
            state.verbosity = value
        return value

    return click.option(
        "-v",
        "--verbose",
        count=True,
        expose_value=False,
        help="Enables verbosity. Use -vv for very verbose (Debug)",
        callback=callback,
    )(f)


def verbosity_option(f):
    def callback(ctx, param, value):
        state = ctx.ensure_object(State)
        if value:
            state.verbosity = value
        return value

    return click.option(
        "--verbosity",
        expose_value=False,
        help="Set verbosity directly [-2:2]",
        callback=callback,
    )(f)


def debug_option(f):
    def callback(ctx, param):
        state = ctx.ensure_object(State)
        if param:
            state.verbosity = param
        return

    return click.option(
        "--debug",
        flag_value=2,
        expose_value=False,
        help="Enables or disables debug mode.",
        callback=callback,
    )(f)


def quiet_option(f):
    def callback(ctx, param):
        state = ctx.ensure_object(State)
        if param:
            state.verbosity = param
        return

    return click.option(
        "--quiet",
        flag_value=-1,
        expose_value=False,
        help="Enables quiet.",
        callback=callback,
    )(f)


def silent_option(f):
    def callback(ctx, param):
        state = ctx.ensure_object(State)
        if param:
            state.verbosity = param
        return

    return click.option(
        "--silent",
        flag_value=-2,
        expose_value=False,
        help="Enables silent.",
        callback=callback,
    )(f)


def common_verbose_options(f):
    f = silent_option(f)
    f = quiet_option(f)
    f = debug_option(f)
    f = verbose_option(f)
    f = verbosity_option(f)
    return f


@click.group()
def cli():
    pass


def application_setup(state, kwargs=None):
    # LOAD: environment, configuration
    default_root_dir = os.path.join(os.path.dirname(__file__))
    app = utils.AppEnv(
        name="ZEPHIR_EXPORT",
        root_dir=os.path.join(os.path.dirname(__file__)),
        kwargs=kwargs,
    )
    app.console = utils.ConsoleMessenger(app="ZEPHIR-EXPORT", verbosity=int(state.verbosity))
    app.console.debug("Loading application...")
    app.console.debug("Environment: {}".format(app.ENV))
    app.console.debug("Configuration: {}".format(app.CONFIG_PATH))
    return app


@cli.command("cids-repo-status")
@common_verbose_options
@pass_state
@click.option(
    "--list-type",
    type=click.Choice(["exists", "missing", "all"], case_sensitive=False),
    default="all",
)
@click.option("--input_path", "-i", type=click.Path(exists=True), required="True")
@click.option("--output_path", "-", type=click.Path(exists=False))
def cids_repo_status_cli(state, **kwargs):
    cids_repo_status_cmd(application_setup(state=state, kwargs=kwargs))


if __name__ == "__main__":
    cli()
