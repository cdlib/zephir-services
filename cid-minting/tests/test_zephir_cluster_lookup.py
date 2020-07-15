import os

import pytest
import environs

from zephir_cluster_lookup import valid_sql_in_clause_str
from zephir_cluster_lookup import invalid_sql_in_clause_str
from zephir_cluster_lookup import ZephirClusterLookupResults
from zephir_cluster_lookup import find_zephir_clusters_by_ocns
from zephir_cluster_lookup import formatting_cid_ocn_clusters

@pytest.fixture
def create_test_db(data_dir, tmpdir, scope="session"):
    db_name = "test_db_for_zephir.db"
    #database = os.path.join(tmpdir, db_name)
    database = os.path.join(data_dir, db_name)
    setup_sql = os.path.join(data_dir, "setup_zephir_test_db.sql")

    cmd = "sqlite3 {} < {}".format(database, setup_sql)
    print(cmd)
    os.system(cmd)

    os.environ["OVERRIDE_DB_CONNECT_STR"] = 'sqlite:///{}'.format(database)
    print("set env: {}".format(os.environ.get("OVERRIDE_DB_CONNECT_STR")))

def test_find_zephir_cluster_by_one_ocn(create_test_db):
    """ the 'create_test_db' argument here is matched to the name of the
        fixture above
    """
    db_conn_str = os.environ.get("OVERRIDE_DB_CONNECT_STR")
    ocns_str = "'8727632'"
    expected_result = [('002492721', '8727632')]
    result = find_zephir_clusters_by_ocns(db_conn_str, ocns_str)
    print(result)
    assert result != None
    assert result == expected_result

def test_find_zephir_cluster_by_ocns(create_test_db):
    """ the 'create_test_db' argument here is matched to the name of the
        fixture above
    """
    db_conn_str = os.environ.get("OVERRIDE_DB_CONNECT_STR")
    print("in test_find_query db_conn_str: {}".format(db_conn_str))

    ocns_str = "'6758168','15437990','5663662','33393343','28477569','8727632'"
    expected_results = [
            ('001693730', '15437990'), 
            ('001693730', '5663662'),
            ('001693730', '6758168'), 
            ('002492721', '8727632'), 
            ('009547317', '28477569'), 
            ('009547317', '33393343'),
        ]

    results = find_zephir_clusters_by_ocns(db_conn_str, ocns_str)
    print(results)
    assert results != None
    assert results == expected_results

def test_formatting_cid_ocn_clusters():
    list_of_tuples = [('001693730', '6758168'), ('001693730', '15437990'), ('001693730', '5663662'), ('002492721', '8727632'), ('009547317', '33393343'), ('009547317', '28477569')]
    formatting_cid_ocn_clusters(list_of_tuples)
    expected_results = {
            '001693730': ['6758168', '15437990', '5663662'], 
            '002492721': ['8727632'],
            '009547317': ['33393343', '28477569'],
            }
    results = formatting_cid_ocn_clusters(list_of_tuples)
    assert results != None
    assert results == expected_results

def test_valid_sql_in_clause_str_invalid():
    input_str = {
        "None": None,
        "Empty": "",
        "one_no_single_quote": "1234",
        "two_no_single_quote": "1, 234",
        "two_no_commna":"'1' '2'",
        "one_not_numbers":"'a'",
        "two_not_numbers":"'ab' 'c'",
    }

    for i, val in input_str.items():
        assert valid_sql_in_clause_str(val) == False

def test_valid_sql_in_clause_str_valid():
    input_str = {
        "one_item":"'1'",
        "one_item_with_space":"'1' ",
        "two_items":"'123', '456'",
        "two_items_with_spaces": "'1' , '234'",
    }

    for i, val in input_str.items():
        assert valid_sql_in_clause_str(val) == True

def test_invalid_sql_in_clause_str():
    valid_str = "'123', '456'";
    assert invalid_sql_in_clause_str(valid_str) == False

    invalid_str = "123"
    assert invalid_sql_in_clause_str(invalid_str) == True