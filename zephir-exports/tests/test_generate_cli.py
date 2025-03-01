import datetime
import filecmp
import os
import sys
import shutil

from freezegun import freeze_time
import pytest

from lib.export_cache import ExportCache
from generate_cli import generate_cli


@pytest.fixture
def env_setup(td_tmpdir, monkeypatch):
    # use tmpdir configuration, export-path and cache-path.
    monkeypatch.setenv(
        "ZEPHIR_OVERRIDE_CONFIG_PATH", os.path.join(str(td_tmpdir), "config")
    )
    monkeypatch.setenv("ZEPHIR_OUTPUT_PATH", td_tmpdir)
    monkeypatch.setenv("ZEPHIR_CACHE_PATH", td_tmpdir)

    if "MYSQL_UNIX_PORT" in os.environ:
        monkeypatch.setenv("ZEPHIR_DB_SOCKET", os.environ["MYSQL_UNIX_PORT"])
    # load test data
    os.system("mysql --host=localhost --user=root  < {}/micro-db.sql".format(td_tmpdir))


def test_required_arguments_enforced(td_tmpdir, env_setup, capsys):
    incomplete_requirements = [
        {"args": ["", "ht-bib-full"], "error": "--merge-version"},
        {"args": ["", "--merge-version", "v3"], "error": "EXPORT_TYPE"},
    ]
    for req_set in incomplete_requirements:
        with pytest.raises(SystemExit) as pytest_e:
            sys.argv = req_set["args"]
            generate_cli()
        out, err = capsys.readouterr()
        print(out, file=sys.stdout)
        print(err, file=sys.stderr)
        assert req_set["error"] in err
        assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 2]


@freeze_time("2019-02-18")
def test_exports_complete(td_tmpdir, env_setup, capsys, pytestconfig, request):
    arg_sets = [
        {"export-type": "ht-bib-full", "merge-version": "v2", "name": "full"},
        {"export-type": "ht-bib-incr", "merge-version": "v3", "name": "incr"},
        {"export-type": "ht-bib-full", "merge-version": "v3", "name": "full"},
        {"export-type": "ht-bib-incr", "merge-version": "v2", "name": "incr"},
    ]
    for arg_set in arg_sets:
        with pytest.raises(SystemExit) as pytest_e:
            sys.argv = [
                "",
                arg_set["export-type"],
                "-mv",
                arg_set["merge-version"],
                "--force",
                "--verbosity",
                pytestconfig.getoption("verbose"),
            ]

            generate_cli()
        print(os.listdir(td_tmpdir))

        assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]
        # compare cache created to reference cache
        new_cache = ExportCache(
            td_tmpdir,
            "cache-{}-{}".format(
                arg_set["merge-version"], datetime.datetime.today().strftime("%Y-%m-%d")
            ),
        )
        ref_cache = ExportCache(
            td_tmpdir, "cache-{}-ref".format(arg_set["merge-version"])
        )
        assert new_cache.size() == ref_cache.size()
        assert hash(new_cache.frozen_content_set()) == hash(
            ref_cache.frozen_content_set()
        )
        export_filename = "ht_bib_export_{}_{}.json".format(
            arg_set["name"], datetime.datetime.today().strftime("%Y-%m-%d")
        )

        assert filecmp.cmp(
            os.path.join(td_tmpdir, export_filename),
            os.path.join(
                td_tmpdir,
                "{}-ht_bib_export_{}_ref.json".format(
                    arg_set["merge-version"], arg_set["name"]
                ),
            ),
        )

        os.remove(os.path.join(td_tmpdir, export_filename))


def test_export_with_alternate_cache_and_output(
    request, td_tmpdir, env_setup, capsys, pytestconfig
):
    # SETUP TODO (cscollett: there may be a better place to put this)
    # set temp current working directory
    real_cwd = os.getcwd()
    os.chdir(td_tmpdir)

    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = [
            "",
            "ht-bib-full",
            "-mv",
            "v3",
            "--cache-path",
            "my_custom_cache.db",
            "--output-path",
            "my_custom_output.json",
            "--force",
            "--verbosity",
            pytestconfig.getoption("verbose"),
        ]

        generate_cli()

    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]

    # compare cache created to reference cache
    new_cache = ExportCache(td_tmpdir, "my_custom_cache")
    ref_cache = ExportCache(td_tmpdir, "cache-v3-ref")
    assert new_cache.size() == ref_cache.size()
    assert hash(new_cache.frozen_content_set()) == hash(ref_cache.frozen_content_set())
    assert filecmp.cmp(
        os.path.join(td_tmpdir, "my_custom_output.json"),
        os.path.join(td_tmpdir, "v3-ht_bib_export_full_ref.json"),
    )
    # CLEANUP
    # unset temp current working directory
    os.chdir(real_cwd)


def test_use_existing_cache(td_tmpdir, env_setup, capsys, pytestconfig):
    # SETUP TODO (cscollett: there may be a better place to put this)
    # set temp current working directory
    real_cwd = os.getcwd()
    os.chdir(td_tmpdir)

    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = [
            "",
            "ht-bib-full",
            "-mv",
            "v3",
            "--cache-path",
            "cache-v3-ref.db",
            "--output-path",
            "my_custom_output.json",
            "--force",
            "--verbosity",
            pytestconfig.getoption("verbose"),
        ]

        generate_cli()

    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]
    # compare cache created to reference cache
    assert filecmp.cmp(
        os.path.join(td_tmpdir, "my_custom_output.json"),
        os.path.join(td_tmpdir, "v3-ht_bib_export_full_ref.json"),
    )
    #     # CLEANUP
    #     # unset temp current working directory
    os.chdir(real_cwd)
