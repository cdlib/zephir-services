import os

import pytest
import environs

from zephir_cluster_lookup import ZephirDatabase
from zephir_cluster_lookup import valid_sql_in_clause_str
from zephir_cluster_lookup import invalid_sql_in_clause_str
from zephir_cluster_lookup import list_to_str
from zephir_cluster_lookup import formatting_cid_id_clusters
from zephir_cluster_lookup import find_zephir_clusters_by_ocns
from zephir_cluster_lookup import find_zephir_clusters_by_cids
from zephir_cluster_lookup import find_zephir_clusters_by_contribsys_ids
from zephir_cluster_lookup import find_zephir_clusters_and_contribsys_ids_by_cid
from zephir_cluster_lookup import zephir_clusters_lookup

@pytest.fixture
def create_test_db(data_dir, tmpdir, scope="session"):
    db_name = "test_db_for_zephir.db"
    #database = os.path.join(tmpdir, db_name)
    database = os.path.join(data_dir, db_name)
    setup_sql = os.path.join(data_dir, "setup_zephir_test_db.sql")

    cmd = "sqlite3 {} < {}".format(database, setup_sql)
    print("Create SQLite db for testing:")
    print(cmd)
    os.system(cmd)

    db_conn_str = 'sqlite:///{}'.format(database)
    
    return ZephirDatabase(db_conn_str)

def test_find_zephir_cluster_by_no_ocn(create_test_db):
    """ the 'create_test_db' argument here is matched to the name of the
        fixture above
    """
    zephirDb = create_test_db
    ocns_list = []

    cid_ocn_list = find_zephir_clusters_by_ocns(zephirDb, ocns_list)
    assert cid_ocn_list == None

def test_find_zephir_cluster_by_ocn_not_in_zephir(create_test_db):
    """ the 'create_test_db' argument here is matched to the name of the
        fixture above
    """
    zephirDb = create_test_db
    ocns_list = [12345678901]

    cid_ocn_list = find_zephir_clusters_by_ocns(zephirDb, ocns_list)
    assert cid_ocn_list == []

def test_find_zephir_cluster_by_one_ocn(create_test_db):
    """ the 'create_test_db' argument here is matched to the name of the
        fixture above
    """
    zephirDb = create_test_db
    ocns_list = [8727632]
    expected_cid_ocn_list = [{"cid": '002492721', "ocn": '8727632'}]

    cid_ocn_list = find_zephir_clusters_by_ocns(zephirDb, ocns_list)
    print(cid_ocn_list)
    assert cid_ocn_list != None
    assert cid_ocn_list == expected_cid_ocn_list

def test_find_zephir_cluster_by_ocns(create_test_db):
    """ the 'create_test_db' argument here is matched to the name of the
        fixture above
    """
    zephirDb = create_test_db
    ocns_list = [6758168, 15437990, 5663662, 33393343, 28477569, 8727632]
    expected_cid_ocn_list = [
            {"cid": '001693730', "ocn": '15437990'}, 
            {"cid": '001693730', "ocn": '5663662'},
            {"cid": '001693730', "ocn": '6758168'}, 
            {"cid": '002492721', "ocn": '8727632'}, 
            {"cid": '009547317', "ocn": '28477569'}, 
            {"cid": '009547317', "ocn": '33393343'}
        ]

    cid_ocn_list = find_zephir_clusters_by_ocns(zephirDb, ocns_list)
    print(cid_ocn_list)
    assert cid_ocn_list != None
    assert cid_ocn_list == expected_cid_ocn_list

def test_find_zephir_cluster_by_cids(create_test_db):
    """ the 'create_test_db' argument here is matched to the name of the
        fixture above
    """
    zephirDb = create_test_db
    cid_list = {
        "one_cid": ['001693730'],
        "two_cids": ['002492721', '009547317'],
        "no_cid": [],
        "not_used_cid": ['123456789'],
    }
    expected_cid_ocn_list = {
        "one_cid": [
                {"cid": '001693730', "ocn": '15437990'}, 
                {"cid": '001693730', "ocn": '5663662'}, 
                {"cid": '001693730', "ocn": '6758168'}],
        "two_cids": [
                {"cid": '002492721', "ocn": '8727632'}, 
                {"cid": '009547317', "ocn": '28477569'}, 
                {"cid": '009547317', "ocn": '33393343'}],
        "no_cid": None,
        "not_used_cid": [],
    }

    for k, cids in cid_list.items():
        cid_ocn_list = find_zephir_clusters_by_cids(zephirDb, cids)
        print(cid_ocn_list)
        assert cid_ocn_list == expected_cid_ocn_list[k]

def test_find_zephir_cluster_by_contribsys_ids(create_test_db):
    """ the 'create_test_db' argument here is matched to the name of the
        fixture above
        Test datasets:
        000000009|miu000000009
        000000009|nrlfGLAD151160146-B
        000000280|nrlfGLAD100908680-B
        000000446|miu000000446
        000002076|miu000002076
        000025463|pur2671999
        000246197|ia-nrlf.b131529626
        000249880|ia-nrlf.b12478852x
        000249880|ia-srlf334843
    """
    zephirDb = create_test_db
    sysid_list = {
        "one_sysid": ['miu000000009'],
        "two_sysids": ['ia-nrlf.b131529626', 'ia-srlf334843'],
        "no_sysid": [],
        "not_used_sysid": ['123456789'],
    }
    expected_cid_sysid_list = {
        "one_sysid": [
                {"cid": '000000009', "contribsys_id": 'miu000000009'},
                ],
        "two_sysids": [
                {"cid": '000246197', "contribsys_id": 'ia-nrlf.b131529626'},
                {"cid": '000249880', "contribsys_id": 'ia-srlf334843'}],
        "no_sysid": None,
        "not_used_sysid": [],
    }

    for k, sysids in sysid_list.items():
        cid_sysid_list = find_zephir_clusters_by_contribsys_ids(zephirDb, sysids)
        print(cid_sysid_list)
        assert cid_sysid_list == expected_cid_sysid_list[k]

def test_find_zephir_cluster_and_contribsys_ids_by_cids(create_test_db):
    """ the 'create_test_db' argument here is matched to the name of the
        fixture above
        Test datasets:
        000000009|miu000000009
        000000009|nrlfGLAD151160146-B
        000000280|nrlfGLAD100908680-B
        000000446|miu000000446
        000002076|miu000002076
        000025463|pur2671999
        000246197|ia-nrlf.b131529626
        000249880|ia-nrlf.b12478852x
        000249880|ia-srlf334843
    """
    zephirDb = create_test_db
    cid_list = {
        "one_cid": ["000000280"],
        "two_cids": ["000000280", "000249880"],
        "no_cid": [],
        "not_used_cid": ["123456789"],
    }
    expected_cid_sysid_list = {
        "one_cid": [
                {"cid": "000000280", "contribsys_id": "nrlfGLAD100908680-B"}],
        "two_cids": [
                {"cid": "000000280", "contribsys_id": "nrlfGLAD100908680-B"},
                {"cid": "000249880", "contribsys_id": "ia-nrlf.b12478852x"},
                {"cid": "000249880", "contribsys_id": "ia-srlf334843"},
                ],
        "no_cid": None,
        "not_used_cid": [],
    }

    for k, cids in cid_list.items():
        cid_sysid_list = find_zephir_clusters_and_contribsys_ids_by_cid(zephirDb, cids)
        print(cid_sysid_list)
        assert cid_sysid_list == expected_cid_sysid_list[k]

def test_zephir_cluster_lookup_no_matched_cluster(create_test_db):
    zephirDb = create_test_db
    ocns_lists = {
        "not_in_zephir": [12345678901],
        "empty": [],
    }

    for k, ocns_list in ocns_lists.items():
        result = zephir_clusters_lookup(zephirDb, ocns_list)
        assert result["cid_ocn_list"] == [] 
        assert result["cid_ocn_clusters"] == {}
        assert result["num_of_matched_zephir_clusters"] == 0 
        assert result["inquiry_ocns_zephir"] == ocns_list
        assert result["min_cid"] == None

def test_zephir_cluster_lookup_matched_1_cluster(create_test_db):
    zephirDb = create_test_db
    ocns_lists = {
        "one_ocn_1_ocn_cluster": [8727632],
        "one_ocn_2_ocns_cluster": [28477569], # Zephir has more OCNs than incoming record
        "two_ocns_3_ocns_cluster": [15437990, 5663662], # Zephir has more OCNs than incoming record
        "with_ocn_not_in_zephir": [25909, 12345678901], 
    }
    expected_cid_ocn_list = {
        "one_ocn_1_ocn_cluster": [{"cid": '002492721', "ocn": '8727632'}],
        "one_ocn_2_ocns_cluster": [
            {"cid": '009547317', "ocn": '28477569'}, 
            {"cid": '009547317', "ocn": '33393343'}],
        "two_ocns_3_ocns_cluster": [
            {"cid": '001693730', "ocn": '15437990'}, 
            {"cid": '001693730', "ocn": '5663662'},
            {"cid": '001693730', "ocn": '6758168'}],
        "with_ocn_not_in_zephir": [
            {"cid": '000000280', "ocn": '217211158'}, 
            {"cid": '000000280', "ocn": '25909'}],
    }
    expected_clusters = {
        "one_ocn_1_ocn_cluster": {'002492721': [ '8727632']},
        "one_ocn_2_ocns_cluster": {'009547317': ['28477569', '33393343']},
        "two_ocns_3_ocns_cluster": {'001693730': ['15437990', '5663662', '6758168']},
        "with_ocn_not_in_zephir": {'000000280': ['217211158', '25909']},
    }
    expected_min_cid = {
        "one_ocn_1_ocn_cluster": '002492721',
        "one_ocn_2_ocns_cluster": '009547317',
        "two_ocns_3_ocns_cluster": '001693730',
        "with_ocn_not_in_zephir": '000000280',
    }

    for k, ocns_list in ocns_lists.items():
        result = zephir_clusters_lookup(zephirDb, ocns_list)
        assert result["cid_ocn_list"] == expected_cid_ocn_list[k]
        assert result["cid_ocn_clusters"] == expected_clusters[k]
        assert result["num_of_matched_zephir_clusters"] == 1 
        assert result["inquiry_ocns_zephir"] == ocns_list
        assert result["min_cid"] == expected_min_cid[k]

def test_zephir_cluster_lookup_matched_more_than_one_clusters(create_test_db):
    zephirDb = create_test_db
    ocns_list = [12345678901, 6758168, 28477569, 8727632, 217211158]
    expected_cid_ocn_list = [
            {"cid": '000000280', "ocn": '217211158'},
            {"cid": '000000280', "ocn": '25909'},
            {"cid": '001693730', "ocn": '15437990'},
            {"cid": '001693730', "ocn": '5663662'},
            {"cid": '001693730', "ocn": '6758168'},
            {"cid": '002492721', "ocn": '8727632'},
            {"cid": '009547317', "ocn": '28477569'},
            {"cid": '009547317', "ocn": '33393343'},
        ]
    expected_clusters = {
            '000000280': ['217211158', '25909'],
            '001693730': ['15437990', '5663662', '6758168'],
            '002492721': [ '8727632'],
            '009547317': ['28477569', '33393343'],
        }
    result = zephir_clusters_lookup(zephirDb, ocns_list)
    assert result["cid_ocn_list"] == expected_cid_ocn_list
    assert result["cid_ocn_clusters"] == expected_clusters
    assert result["num_of_matched_zephir_clusters"] == 4
    assert result["inquiry_ocns_zephir"] == ocns_list
    assert result["min_cid"] == '000000280'

def test_list_to_str():
    input_list = {
        "1_item_int": [123],
        "2_items_int": [123, 456],
        "str_items_single_quoted": ['123', '456'],
        "str_items_double_quoted": ["123", "456"],
        "empty_list": [],
    }
    expected = {
        "1_item_int": "'123'",
        "2_items_int": "'123', '456'",
        "str_items_single_quoted": "'123', '456'",
        "str_items_double_quoted": "'123', '456'",
        "empty_list": "",
    }
    for k, val in input_list.items():
        assert expected[k] == list_to_str(val)

def test_formatting_cid_id_clusters():
    list_of_cid_ocn = [
            {"cid": '001693730', "ocn": '6758168'}, 
            {"cid": '001693730', "ocn": '15437990'},
            {"cid": '001693730', "ocn": '5663662'},
            {"cid": '002492721', "ocn": '8727632'},
            {"cid": '009547317', "ocn": '33393343'},
            {"cid": '009547317', "ocn": '28477569'}]
    expected_results = {
            '001693730': ['6758168', '15437990', '5663662'], 
            '002492721': ['8727632'],
            '009547317': ['33393343', '28477569'],
            }
    results = formatting_cid_id_clusters(list_of_cid_ocn, "ocn")
    assert results != None
    assert results == expected_results

def test_valid_sql_in_clause_str_invalid():
    input_str = [
        None,
        "",
        " ",
        "   ",
        "1234",
        " 1234",
        "1234  ",
        " 1234  ",
        "12'34",
        "12''34",
        "'1234 ",
        "1234' ",
        "1, 234",
        " 1, 234",
        "1, 234 ",
        " 1, 234 ",
        "  1, 234  ",
        "'ab123', '123',",
        " 'ab123', '123',",
        "'ab123', '123', ",
        " 'ab123', '123',  ",
    ]

    for item in input_str:
        assert valid_sql_in_clause_str(item) == False


def test_valid_sql_in_clause_str_valid():
    input_str =[ 
        "'1'",
        "'1' ",
        " '1'",
        " '1' ",
        "  '1'  ",
        "'a'",
        "'ab123'",
        "'ab123' ",
        " 'ab123'",
        " 'ab123' ",
        "  'ab123'  ",
        "'ab123' '123'",
        " 'ab123'  '123' ",
        "  'ab123'  '123'  ",
        "'ab123','123'",
        "'ab123', '123'",
        "'ab123', '123' ",
        " 'ab123', '123'",
        " 'ab123', '123' ",
        "  'ab123',  '123'  ",
        "'1''2abc'",
    ]

    for item in input_str:
        assert valid_sql_in_clause_str(item) == True

def test_invalid_sql_in_clause_str():
    valid_str = "'123', '456'";
    assert invalid_sql_in_clause_str(valid_str) == False

    invalid_str = "123"
    assert invalid_sql_in_clause_str(invalid_str) == True
