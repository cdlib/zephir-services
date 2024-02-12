import argparse
import sys  # Make sure to import sys for sys.exit()

def example_cli(echo):
    # Directly print the echo argument to the console
    print(echo)

def main():
    parser = argparse.ArgumentParser(description="Example simple CLI.")
    # Include arguments here
    parser.add_argument("echo", help="Echo the string provided as an argument")

    # Parse arguments
    args = parser.parse_args()  # This line was missing, causing args to be undefined.

    try:
        # Since there's no application setup needed anymore, directly call example_cli
        # run command with argument
        example_cli(args.echo)
    except Exception as err:
        print(err, file=sys.stderr)
        sys.exit(1)  # Use sys.exit to raise SystemExit

    sys.exit(0)  # Ensure to call sys.exit(0) to explicitly exit with code 0

if __name__ == "__main__":
    main()
