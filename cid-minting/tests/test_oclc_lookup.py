import os
import ast
import sqlite3

import pytest
from sqlite_concordance import create_concordance_db
from click.testing import CliRunner

from oclc_lookup import get_primary_ocn
from oclc_lookup import get_ocns_cluster_by_primary_ocn
from oclc_lookup import get_ocns_cluster_by_ocn
from oclc_lookup import get_clusters_by_ocns
from oclc_lookup import convert_set_to_list
from oclc_lookup import lookup_ocns_from_oclc
from oclc_lookup import main

# TESTS
def test_get_primary_ocn(setup):
    concordance_db_path = setup["concordance_db_path"]

    input = list(setup["dfs"]["primary.csv"]["ocn"])
    expect = list(setup["dfs"]["primary.csv"]["primary"])
    result = [
        get_primary_ocn(ocn, concordance_db_path)
        for ocn in input
    ]
    assert sorted(expect) == sorted(result)

def test_get_primary_ocn_with_null_cases(setup):
    concordance_db_path = setup["concordance_db_path"]

    # case: ocn passed is None
    result = get_primary_ocn(None, concordance_db_path)
    assert result == None
    # case: ocn not in the database
    result = get_primary_ocn(0, concordance_db_path)
    assert result == None


def test_concordance_must_exist_and_have_reverse_index(tmpdir):
    missing = os.path.join(str(tmpdir), "missing.sqlite")
    with pytest.raises(FileNotFoundError):
        get_primary_ocn(1, missing)
    assert not os.path.exists(missing)

    incomplete = os.path.join(str(tmpdir), "incomplete.sqlite")
    with sqlite3.connect(incomplete) as db:
        db.execute("CREATE TABLE mapping (variant INTEGER PRIMARY KEY, canonical INTEGER NOT NULL)")
    with pytest.raises(ValueError, match="reverse lookup index"):
        get_primary_ocn(1, incomplete)

def test_get_ocns_cluster_by_primary_ocn(setup):
    concordance_db_path = setup["concordance_db_path"]

    primary_ocn = 1
    cluster = [9987701, 53095235, 433981287, 6567842]
    result = get_ocns_cluster_by_primary_ocn(primary_ocn, concordance_db_path)
    assert sorted(cluster) == sorted(result)

def test_get_cluster_missing_primary(setup):
    concordance_db_path = setup["concordance_db_path"]

    primary_ocn = 1
    result = get_ocns_cluster_by_primary_ocn(primary_ocn, concordance_db_path)
    assert primary_ocn not in result

def test_get_ocns_cluster_by_primary_ocn_2(setup):
    concordance_db_path = setup["concordance_db_path"]

    primary_ocn = 17216714 
    cluster = [535434196]
    result = get_ocns_cluster_by_primary_ocn(primary_ocn, concordance_db_path)
    assert sorted(cluster) == sorted(result)

def test_get_cluster_ocn_with_null_cases(setup):
    concordance_db_path = setup["concordance_db_path"]

    null_cases = {
        "cluster_of_one_ocn": 1000000000,
        "secondary_ocn": 6567842,
        "invalid_ocn": 1234567890,
        "none_ocn": None,
    }
    for k,v in null_cases.items():
        assert None == get_ocns_cluster_by_primary_ocn(v, concordance_db_path)
        
def test_get_ocns_cluster_by_ocn(setup):
    concordance_db_path = setup["concordance_db_path"]

    clusters = {
        # ocn: list of all ocns of the cluster
        1000000000: [1000000000],                               # cluster_of_one_ocn
        1: [6567842, 9987701, 53095235, 433981287, 1],          # cluster_of_multi_ocns_by_primary_ocn
        6567842: [1, 6567842, 9987701, 53095235, 433981287],    # cluster_of_multi_ocns_by_other_ocn
        17216714: [17216714, 535434196],                        # cluster_of_2_ocns_by_primary_ocn, 
    }
    for ocn, cluster in clusters.items():
        result = get_ocns_cluster_by_ocn(ocn, concordance_db_path)
        assert sorted(cluster) == sorted(result)

def test_get_ocns_cluster_by_ocn_with_null_cases(setup):
    concordance_db_path = setup["concordance_db_path"]

    null_cases = {
        "invalid_ocn": 1234567890,
        "none_ocn": None,
    }
    for k, v in null_cases.items():
        assert None == get_ocns_cluster_by_ocn(v, concordance_db_path)

def test_get_ocns_cluster_by_ocns(setup):
    concordance_db_path = setup["concordance_db_path"]

    clusters = {
        # primary_ocn, list of all ocns of the cluster 
        1000000000: [1000000000],                               # cluster_of_one_ocn
        1: [6567842, 9987701, 53095235, 433981287, 1],          # cluster_of_multi_ocns
        17216714: [17216714, 535434196],                        # cluster_of_2_ocns,
    }
    sets = {
        1000000000: {(1000000000,)},
        1: {(1, 6567842, 9987701, 53095235, 433981287)},
        17216714: {(17216714, 535434196)},
    }
    input_ocns_list = {
        "1_one_primary_ocn_cluster_of_one": [1000000000],
        "2_one_other_ocn_cluster_of_multi": [6567842],
        "3_two_primary_ocns_dups": [1000000000, 1000000000],
        "4_two_primary_ocns": [1, 1000000000],
        "5_ocns_with_primary_secondary_dups_invalid": [1, 1, 6567842, 17216714, 535434196, 12345678901, 1000000000],
    }
    expected_set = {
        "1_one_primary_ocn_cluster_of_one": sets[1000000000],
        "2_one_other_ocn_cluster_of_multi": sets[1],
        "3_two_primary_ocns_dups": sets[1000000000],
        "4_two_primary_ocns": (sets[1] | sets[1000000000]),
        "5_ocns_with_primary_secondary_dups_invalid": (sets[1] | sets[17216714] | sets[1000000000]),
    }
    
    for k, ocns in input_ocns_list.items():
        result = get_clusters_by_ocns(ocns, concordance_db_path)
        print(result)
        assert result != None
        assert result == expected_set[k] 

def test_get_ocns_cluster_by_ocns_wthnull_cases(setup):
    concordance_db_path = setup["concordance_db_path"]

    input_ocns_list = {
        "one_invalid_ocn": [1234567890],
        "two_invalid_ocns": [1234567890, 12345678901],
        "no_ocns": [],
    }
    for k, ocns in input_ocns_list.items():
        result = get_clusters_by_ocns(ocns, concordance_db_path)
        assert result == set()

def test_convert_set_to_list():
    input_sets = {
        "one_tuple_single_item": {(1000000000,)},
        "one_tuple_multi_items": {(1, 6567842, 9987701, 53095235, 433981287)},
        "two_tuples": {(1000000000,), (1, 6567842, 9987701, 53095235, 433981287)},
        "empty_set": set(),
    }
    expected_lists = {
        "one_tuple_single_item": [[1000000000]],
        "one_tuple_multi_items": [[1, 6567842, 9987701, 53095235, 433981287]],
        "two_tuples": [[1000000000], [1, 6567842, 9987701, 53095235, 433981287]],
        "empty_set": []
    }
    for k, a_set in input_sets.items():
        assert convert_set_to_list(a_set) == expected_lists[k]


def test_lookup_ocns_from_oclc(setup):
    concordance_db_path = setup["concordance_db_path"]

    input_ocns = {
        "one_ocn_primary_single_cluster": [1000000000],
        "one_ocn_primary_multi_cluster": [1],
        "one_other_ocn": [6567842],
        "two_ocns": [1000000000, 6567842],
        "one_invalid": [1234567890],
        "two_invalid": [1234567890, 12345678901],
    }

    expected = {
        "one_ocn_primary_single_cluster": {
            "inquiry_ocns": [1000000000],
            "matched_oclc_clusters": [[1000000000]],
            "num_of_matched_oclc_clusters": 1,
            },
        "one_ocn_primary_multi_cluster": {
            "inquiry_ocns": [1],
            "matched_oclc_clusters": [[1, 6567842, 9987701, 53095235, 433981287]],
            "num_of_matched_oclc_clusters": 1,
            },
        "one_other_ocn": {
            "inquiry_ocns": [6567842],
            "matched_oclc_clusters": [[1, 6567842, 9987701, 53095235, 433981287]],
            "num_of_matched_oclc_clusters": 1,
            },
        "two_ocns": {
            "inquiry_ocns": [1000000000, 6567842],
            "matched_oclc_clusters": [[1000000000], [1, 6567842, 9987701, 53095235, 433981287]],
            "num_of_matched_oclc_clusters": 2,
            },
        "one_invalid": {
            "inquiry_ocns": [1234567890],
            "matched_oclc_clusters": [],
            "num_of_matched_oclc_clusters": 0,
            },
        "two_invalid": {
            "inquiry_ocns": [1234567890, 12345678901],
            "matched_oclc_clusters": [],
            "num_of_matched_oclc_clusters": 0,
            },
    }

    for k, ocns in input_ocns.items():
        result = lookup_ocns_from_oclc(ocns, concordance_db_path)
        assert result["inquiry_ocns"] == ocns
        assert result["matched_oclc_clusters"] == expected[k]["matched_oclc_clusters"]
        assert result["num_of_matched_oclc_clusters"] == expected[k]["num_of_matched_oclc_clusters"]

# TEST cmd line options
def test_main(setup):

    runner = CliRunner()
    result = runner.invoke(main)
    assert result.exit_code == 1 
    assert 'Usage' in result.output

    result = runner.invoke(main, ['1'])
    assert ast.literal_eval(result.output) == {(1, 6567842, 9987701, 53095235, 433981287)}

    result = runner.invoke(main, ['2'])
    assert ast.literal_eval(result.output) == {(2, 9772597, 35597370, 60494959, 813305061, 823937796, 1087342349)}

    result = runner.invoke(main, ['1', '2'])
    assert ast.literal_eval(result.output) == {
        (1, 6567842, 9987701, 53095235, 433981287),
        (2, 9772597, 35597370, 60494959, 813305061, 823937796, 1087342349),
    }

    # '123' is not in the test db
    result = runner.invoke(main, ['123'])
    assert result.output == 'set()\n'

# FIXTURES
@pytest.fixture
def setup(tmpdatadir, csv_to_df_loader):
    dfs = csv_to_df_loader
    concordance_db_path = create_concordance_db(tmpdatadir, dfs["primary.csv"])
    os.environ["OVERRIDE_CONCORDANCE_DB_PATH"] = concordance_db_path

    return {
        "tmpdatadir": tmpdatadir,
        "dfs": dfs,
        "concordance_db_path": concordance_db_path
    }
