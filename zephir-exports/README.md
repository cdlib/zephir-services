## Zephir-Exports

Zephir-exports is a command-line program to export data from Zephir in various formats for use by HathiTrust and contributors. It is designed to be used mainly by cron for daily exports.
See [docs/specs](docs/specs) folder for more information.

### Configuration
Zephir-export requires access to the Zephir database using a MySQL library. The configration location is listed in the `--help` command.

### Use

**Help**: Run for authoritative documentation.

` zephir-exports --help `

**List**: List available exports

*Not implemented yet*

**Generate**: Command to generate the exports.

Running from the code:

`pipenv run python zephir-exports generate <export> `

Running from cron:

`/bin/bash -l -c 'PIPENV_PIPFILE=<Pipfile path location> pipenv run python <zephir-export path location>`

**Compare-Cache**: Compare export caches for content differences

`pipenv run python zephir-export compare-cache <cache-file 1> <cache-file 2>`

*This option is for production scale regression testing*

### Testing
A Pytest test suite is available to run.

`pipenv run pytest -v`

### TODO (suggestions for the future)

- [ ] New CLI options
  - [ ] List available exports
  - [ ] Option to pass cache for use
  - [ ] Option to only generate cache
  - [ ] Option to name output filepath
- [ ] Refactor program structure
  - [ ] Use one configuration
  - [ ] Use plugin, class-based design
  - [ ] Create python package for distribution
- [ ] Add ZED logging
- [ ] Move all exports in CLI (now in ruby)
