import argparse
import sys  

def example_cli(echo):
    # Directly print the echo argument to the console
    print(echo)
    return 0

def main():
    parser = argparse.ArgumentParser(description="Example simple CLI.")
    # Include arguments here
    parser.add_argument("echo", help="Echo the string provided as an argument")

    # Parse arguments
    args = parser.parse_args() 

    try:
        # run command with argument
        return_code = example_cli(args.echo)
    except Exception as err:
        print(err, file=sys.stderr)
        sys.exit(1) 

    sys.exit(return_code) 

if __name__ == "__main__":
    main()
