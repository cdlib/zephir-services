import os
import shutil
import sys

import pytest

from audit import main


@pytest.fixture
def prep_data(tmpdir, monkeypatch):
    # set env so audit script can find unit test temp directory
    monkeypatch.setenv("PYTEST_TMPDIR", str(tmpdir))
    # copy data to test-contained tmp dir
    info = {"dir": tmpdir}
    for entry in os.scandir(os.path.join(os.path.dirname(__file__), "test_audit")):
        if entry.is_file():
            shutil.copy(entry, os.path.join(info["dir"], entry.name))
    return info


def test_audit_errors_with_no_files(td_tmpdir, capsys):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = [""]
        main()
    out, err = capsys.readouterr()
    assert "No files given to process." in err
    assert pytest_e.type, pytest_e.value.code == [SystemExit, 1]


def test_audit_passes_received_events(td_tmpdir, capsys):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ["", os.path.join(td_tmpdir, "found_events.log")]
        main()
    out, err = capsys.readouterr()
    assert "found_events.log: pass" in err
    assert os.path.isfile(os.path.join(td_tmpdir, "found_events.log.audited"))
    assert pytest_e.type, pytest_e.value.code == [SystemExit, 0]


def test_audit_respects_dry_run(td_tmpdir, capsys):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ["", os.path.join(td_tmpdir, "found_events.log"), "--dry-run"]
        main()
    out, err = capsys.readouterr()
    assert "found_events.log: pass" in err
    assert not os.path.isfile(
        os.path.join(td_tmpdir, "found_events.log.validated")
    )
    assert os.path.isfile(os.path.join(td_tmpdir, "found_events.log"))
    assert pytest_e.type, pytest_e.value.code == [SystemExit, 0]


def test_audit_will_not_overwrite(td_tmpdir, capsys):
    shutil.copy(
        os.path.join(td_tmpdir, "found_events.log"),
        os.path.join(td_tmpdir, "found_events.log.audited"),
    )
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ["", os.path.join(td_tmpdir, "found_events.log")]
        main()
    out, err = capsys.readouterr()
    assert "found_events.log: pass" not in err
    assert pytest_e.type, pytest_e.value.code == [SystemExit, 0]


def test_audit_fails_missing_events(td_tmpdir, capsys):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ["", os.path.join(td_tmpdir, "missing_events.log")]
        main()
    out, err = capsys.readouterr()
    # Good data in the test_audit database
    assert "a1baa562" not in err
    assert "b834194c" not in err
    assert "1542f8fd" not in err
    assert "b93cec5b" not in err
    # Bad data not in the test_audit database
    assert "does-not-exist-1" in err
    assert "does-not-exist-2" in err
    assert "missing_events.log: fail" in err
    assert not os.path.isfile(
        os.path.join(td_tmpdir, "missing_events.log.audited")
    )
    assert pytest_e.type, pytest_e.value.code == [SystemExit, 0]


def test_audit_handles_success_and_failure(td_tmpdir, capsys):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = [
            "",
            os.path.join(td_tmpdir, "found_events.log"),
            os.path.join(td_tmpdir, "missing_events.log"),
        ]
        main()
    out, err = capsys.readouterr()
    assert "pass" in err
    assert "fail" in err
    assert os.path.isfile(os.path.join(td_tmpdir, "found_events.log.audited"))
    assert not os.path.isfile(
        os.path.join(td_tmpdir, "missing_events.log.audited")
    )
    assert pytest_e.type, pytest_e.value.code == [SystemExit, 0]


def test_audit_handles_invalid_json(td_tmpdir, capsys):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ["", os.path.join(td_tmpdir, "events_with_invalid_json.log")]
        main()
    out, err = capsys.readouterr()
    assert "fail" in err
    assert not os.path.isfile(
        os.path.join(td_tmpdir, "events_with_invalid_json.log.audited")
    )
    assert pytest_e.type, pytest_e.value.code == [SystemExit, 0]
