import argparse
import sys


def echo_cli():
    """Echo CLI
    Arguments:
        echo: String to echo in stdout

    Output:
        Input written to stdout
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("echo", type=str, help="Input to write to stdout")
    args = parser.parse_args()

    try:
        # run command
        return_code = echo_cmd(args.echo)
    except Exception as err:
        return_code = 1
        raise err
    sys.exit(return_code)


def echo_cmd(echo):
    print("{}".format(echo), file=sys.stdout)
    return 0


if __name__ == "__main__":
    echo_cli()
