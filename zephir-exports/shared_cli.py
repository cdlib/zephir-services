from datetime import datetime
import os
import glob
import sys

import click

import lib.utils_refactor as utils


# COMMON OPTION BLOCK
# use state object to hold argument and option values
class State(object):
    def __init__(self):
        self.verbosity = 0
        self.input_filepaths = []
        self.args = {}


pass_state = click.make_pass_decorator(State, ensure=True)

# define options to handle verbosity in reusable decorators
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
        help="Enables info-level messages. Use -vv for very verbose (debug-level)",
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
    def callback(ctx, param, value):
        state = ctx.ensure_object(State)
        if value:
            state.verbosity = value
        return

    return click.option(
        "--debug",
        flag_value=2,
        expose_value=False,
        help="Enables or disables debug-level messages in stderr",
        callback=callback,
    )(f)


def quiet_option(f):
    def callback(ctx, param, value):
        state = ctx.ensure_object(State)
        if value:
            state.verbosity = value
        return

    return click.option(
        "--quiet",
        flag_value=-1,
        expose_value=False,
        help="Enables quiet (no stdout, error-level in stderr)",
        callback=callback,
    )(f)


def silent_option(f):
    def callback(ctx, param, value):
        state = ctx.ensure_object(State)
        if value:
            state.verbosity = value
        return

    return click.option(
        "--silent",
        flag_value=-2,
        expose_value=False,
        help="Enables silent (no stdout or stderr)",
        callback=callback,
    )(f)

# group common verbose options
def common_verbose_options(f):
    f = silent_option(f)
    f = quiet_option(f)
    f = debug_option(f)
    f = verbose_option(f)
    f = verbosity_option(f)
    return f

# custom callback to inspect filepaths, with and without date substitution
def path_callback(ctx, param, value):
    state = ctx.ensure_object(State)
    paths = value
    if paths:
        if type(paths) == str:
            # make single path iterable
            paths = [paths]
        for idx, path in enumerate(paths):
            if os.path.exists(path):
                state.input_filepaths.append(path)
            else:
                date_path = datetime.now().strftime(path)
                if date_path != path:
                    # file not found, try date-formatted filepath if differnt
                    if os.path.exists(date_path):
                        state.input_filepaths.append(date_path)
                    else:
                        # possibly date-format intended, error both file forms
                        raise click.ClickException(
                            "Files not found: {}, {} (date-modified)".format(
                                path, date_path
                            )
                        )
                raise click.ClickException("File not found: {}".format(path))
        return
