# no argument
def test_main_param_err_0(capsys, create_test_db):
    with pytest.raises(SystemExit) as pytest_e:
        main()
    out, err = capsys.readouterr()
    assert "Parameter error" in out
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 1]

# one argument
def test_main_param_err_1(capsys, create_test_db):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ['']
        main()
    out, err = capsys.readouterr()
    assert "Parameter error" in out
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 1]

def test_main_param_err_2(capsys, create_test_db):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ['', 'dev']
        main()
    out, err = capsys.readouterr()
    assert "Parameter error" in out
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 1]

def test_main_read_by_ocns(capsys, create_test_db):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ['', 'test', 'read', 'ocn', '8727632,32882115']
        main()
    out, err = capsys.readouterr()
    expected = '{"inquiry_ocns": ["8727632", "32882115"], "matched_cids": [{"cid": "011323405"}, {"cid": "002492721"}], "min_cid": "002492721", "num_of_cids": 2}'
    assert expected in out
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]

def test_main_read_by_ocns_not_exists(capsys, create_test_db):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ['', 'test', 'read', 'ocn', '1234567890']
        main()
    out, err = capsys.readouterr()
    expected = '{"inquiry_ocns": ["1234567890"], "matched_cids": [], "min_cid": null, "num_of_cids": 0}'
    assert expected in out
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]

def test_main_read_by_sysid(capsys, create_test_db):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ['', 'test', 'read', 'sysid', 'pur215476']
        main()
    out, err = capsys.readouterr()
    expected = '{"inquiry_sys_id": "pur215476", "matched_cid": "002492721"}'
    assert expected in out
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]

def test_main_read_by_sysid_not_exists(capsys, create_test_db):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ['', 'test', 'read', 'sysid', 'sysid123']
        main()
    out, err = capsys.readouterr()
    expected = '{}'
    assert expected in out
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]

def test_main_write_ocn(capsys, create_test_db):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ['', 'test', 'write', 'ocn', '123456789', '100000000']
        main()
    out, err = capsys.readouterr()
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]

def test_main_write_sysid(capsys, create_test_db):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ['', 'test', 'write', 'sysid', 'XY1234567', '200000000']
        main()
    out, err = capsys.readouterr()
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]

# 3|ocn|32882115|011323405|2020-09-16 00:09:26
def test_main_write_ocn_dup(capsys, create_test_db):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ['', 'test', 'write', 'ocn', '32882115', '011323405']
        main()
    out, err = capsys.readouterr()
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 1]

# 4|sysid|pur864352|011323405|2020-09-16 00:09:26
def test_main_write_sysid_dup(capsys, create_test_db):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ['', 'test', 'write', 'sysid', 'pur864352', '011323405']
        main()
    out, err = capsys.readouterr()
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 1]

