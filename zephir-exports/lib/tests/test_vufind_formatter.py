import json
import os
import shutil
import sys

import pytest

from vufind_formatter import VufindFormatter


def test_vufind_successfully_merged(td_tmpdir):
    records = []
    with open(os.path.join(td_tmpdir, "000000001.json"), "r") as f:
        for line in f:
            records.append(line)

    vufind_record = VufindFormatter.create_record("000000001", records)
    assert len(vufind_record.get_fields("974")) == 5
    assert len(vufind_record.get_fields("035")) == 7
    assert vufind_record.get_fields("974")[0]["u"] == "mdp.39015018415946"
