"""Look up OCLC clusters in the configured SQLite concordance database."""

import os

import click

from lib.utils import get_configs_by_filename
from cid_minter.oclc_lookup import get_clusters_by_ocns


@click.command()
@click.argument("ocns", nargs=-1, type=int)
def main(ocns):
    if not ocns:
        click.echo(click.get_current_context().get_help())
        raise SystemExit(1)
    config_dir = os.path.join(os.path.dirname(__file__), "config")
    config = get_configs_by_filename(config_dir, "cid_minting")
    db_path = os.environ.get("OVERRIDE_CONCORDANCE_DB_PATH") or config["concordance_db_path"]
    click.echo(get_clusters_by_ocns(ocns, db_path))


if __name__ == "__main__":
    main()
