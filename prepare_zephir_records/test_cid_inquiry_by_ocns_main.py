# no argument
def test_main_param_err_0(capsys, setup_leveldb, setup_sqlite):
    with pytest.raises(SystemExit) as pytest_e:
        main()
    out, err = capsys.readouterr()
    assert "Parameter error" in out
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 1]

# one argument
def test_main_param_err_1(capsys, setup_leveldb, setup_sqlite):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ['dev']
        main()
    out, err = capsys.readouterr()
    assert "Parameter error" in out
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 1]

# two arguments
def test_main_param_err_2(capsys, setup_leveldb, setup_sqlite):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ['dev', '1']
        main()
    out, err = capsys.readouterr()
    assert "Parameter error" in out
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 1]

# OCN: 300 (not in test db) 
def test_main_not_in_test_db(capsys, setup_leveldb, setup_sqlite):
    with pytest.raises(SystemExit) as pytest_e:
        # env will be override by environment varaiable
        sys.argv = ['', 'test', '300']
        main()
    out, err = capsys.readouterr()
    expected = '{"inquiry_ocns": [300], "matched_oclc_clusters": [[300, 39867290, 39867383]], "num_of_matched_oclc_clusters": 1, "inquiry_ocns_zephir": [300, 39867290, 39867383], "cid_ocn_list": [], "cid_ocn_clusters": {}, "num_of_matched_zephir_clusters": 0, "min_cid": null}'

    assert  expected in out
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]

# OCNs: 217211158, 8727632 (test case 1&2 c)
def test_main_in_test_db_2_clusters(capsys, setup_leveldb, setup_sqlite):
    with pytest.raises(SystemExit) as pytest_e:
        # env will be override by environment varaiable
        sys.argv = ['', 'test', '217211158,8727632']
        main()
    out, err = capsys.readouterr()

    expected = '{"inquiry_ocns": [217211158, 8727632], "matched_oclc_clusters": [[8727632, 24253253]], "num_of_matched_oclc_clusters": 1, "inquiry_ocns_zephir": [8727632, 24253253, 217211158], "cid_ocn_list": [{"cid": "000000280", "ocn": "217211158"}, {"cid": "000000280", "ocn": "25909"}, {"cid": "002492721", "ocn": "8727632"}], "cid_ocn_clusters": {"000000280": ["217211158", "25909"], "002492721": ["8727632"]}, "num_of_matched_zephir_clusters": 2, "min_cid": "000000280"}'

    assert expected in out
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]

