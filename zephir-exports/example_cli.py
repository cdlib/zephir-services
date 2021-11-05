import click  # include for cmd line framework
import os  # include for

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
@click.command("example")
# pass state variable used in verbose options
@pass_state
# include verbose options defined in shared_cli
@common_verbose_options
# include arguments here
@click.argument("echo")
def example_cli(state, echo, **kwargs):
    """Example CLI for expansion."""
    try:
        # setup an application given state and kwargs
        app = utils.application_setup(
            root_dir=os.path.join(os.path.dirname(__file__)), state=state, kwargs=kwargs
        )
        # run command with application context, and argument
        return_code = example_cmd(app, echo)
    except Exception as err:
        return_code = 1
        raise click.ClickException(err)
    click.Context.exit(return_code)


def example_cmd(app, echo):
    # run business logic
    app.console.out("{}".format(echo))


if __name__ == "__main__":
    compare_file_cli()
