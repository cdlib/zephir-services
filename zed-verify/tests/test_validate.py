import os
import shutil
import sys

import pytest

from validate import validate


@pytest.fixture
def prep_data(tmpdir, monkeypatch):
    # copy data to test-contained tmp dir
    info = {"dir": tmpdir}
    for entry in os.scandir(os.path.join(os.path.dirname(__file__), "test_validate")):
        if entry.is_file():
            shutil.copy(entry, os.path.join(info["dir"], entry.name))
    return info


def test_validate_errors_with_no_files(capsys):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = [""]
        validate()
    out, err = capsys.readouterr()
    assert "No files given to process" in err
    assert[pytest_e.type, pytest_e.value.code]== [SystemExit, 1]


def test_validate_passes_valid_json(prep_data, capsys):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ["", os.path.join(prep_data["dir"], "valid.log")]
        validate()
    out, err = capsys.readouterr()
    assert "valid.log: valid" in err
    assert os.path.isfile(os.path.join(prep_data["dir"], "valid.log.validated"))
    assert[pytest_e.type, pytest_e.value.code]== [SystemExit, 0]


def test_validate_respects_dry_run(prep_data, capsys):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ["", os.path.join(prep_data["dir"], "valid.log"), "--dry-run"]
        validate()
    out, err = capsys.readouterr()
    assert "valid.log: valid" in err
    assert not os.path.isfile(os.path.join(prep_data["dir"], "valid.log.validated"))
    assert os.path.isfile(os.path.join(prep_data["dir"], "valid.log"))
    assert[pytest_e.type, pytest_e.value.code]== [SystemExit, 0]


def test_validate_will_not_overwrite(prep_data, capsys):
    shutil.copy(
        os.path.join(prep_data["dir"], "valid.log"),
        os.path.join(prep_data["dir"], "valid.log.validated"),
    )
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ["", os.path.join(prep_data["dir"], "valid.log")]
        validate()
    out, err = capsys.readouterr()
    assert "valid.log: valid" not in err
    assert[pytest_e.type, pytest_e.value.code]== [SystemExit, 0]


def test_validate_fails_invalid_json(prep_data, capsys):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ["", os.path.join(prep_data["dir"], "invalid_json.log")]
        validate()
    out, err = capsys.readouterr()
    assert "invalid_json.log: invalid" in err
    assert not os.path.isfile(os.path.join(prep_data["dir"], "invalid.log.validated"))
    assert[pytest_e.type, pytest_e.value.code]== [SystemExit, 0]


def test_validate_fails_invalid_zed_schema_json(prep_data, capsys):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ["", os.path.join(prep_data["dir"], "invalid_zed_json.log")]
        validate()
    out, err = capsys.readouterr()
    assert "Validation error" in err
    assert "invalid_zed_json.log: invalid" in err
    assert not os.path.isfile(
        os.path.join(prep_data["dir"], "invalid_zed_json.log.validated")
    )
    assert[pytest_e.type, pytest_e.value.code]== [SystemExit, 0]


def test_validate_fails_duplicate_id(prep_data, capsys):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ["", os.path.join(prep_data["dir"], "duplicate.log"), "--verbose"]
        validate()
    out, err = capsys.readouterr()
    assert "Duplicate" in err
    assert "duplicate.log: invalid" in err
    assert not os.path.isfile(os.path.join(prep_data["dir"], "duplicate.log.validated"))
    assert[pytest_e.type, pytest_e.value.code]== [SystemExit, 0]


def test_validate_handles_success_and_failure(prep_data, capsys):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = [
            "",
            os.path.join(prep_data["dir"], "valid.log"),
            os.path.join(prep_data["dir"], "duplicate.log"),
            os.path.join(prep_data["dir"], "invalid_json.log"),
        ]
        validate()
    out, err = capsys.readouterr()
    assert ": invalid" in err
    assert ": valid" in err
    assert[pytest_e.type, pytest_e.value.code]== [SystemExit, 0]
