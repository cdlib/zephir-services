import sys


class ConsoleMessenger:
    def __init__(self, quiet=False, verbose=False):
        self.quiet = quiet
        self.verbose = verbose

    # verbose diagnostic messages only
    def diagnostic(self, message):
        if self.verbose:
            print(message, file=sys.stderr)

    # concise operational reporting
    def report(self, message):
        if not self.quiet:
            print(message, file=sys.stderr)

    # concise error handling messages
    def error(self, message):
        print(message, file=sys.stderr)

    # standard output for use by chained applications
    def out(self, message):
        if not self.quiet:
            print(message, file=sys.stdout)
