import os
import sys

import msgpack
import plyvel

from resolution_table import ResolutionTable


def test_dtype_transform_lamdbas(tmpdir):
    rt = ResolutionTable(os.path.join(tmpdir, "test/"))

    # int
    int_test = 10
    assert int_test.to_bytes(
        (int_test.bit_length() + 7) // 8, "big"
    ) == rt.to_bytes_func(int)(int_test)
    assert int_test == rt.from_bytes_func(int)(
        int_test.to_bytes((int_test.bit_length() + 7) // 8, "big")
    )
    # str
    str_test = "ten"
    assert str_test.encode("utf-8") == rt.to_bytes_func(str)(str_test)
    assert str_test == rt.from_bytes_func(str)(str_test.encode("utf-8"))
    # else [use msgpack] (list, set, dict, float)
    other_dtypes = {list: [1, 2, 3], float: 1.23, dict: {"123": 123}, tuple: (1, 2, 3)}
    for dtype, value in other_dtypes.items():
        assert msgpack.Packer().pack(value) == rt.to_bytes_func(dtype)(value)
        assert value == dtype(rt.from_bytes_func(dtype)(msgpack.Packer().pack(value)))


def test_extended_dtypes_in_init(tmpdir):
    rt = ResolutionTable(os.path.join(tmpdir, "test/"), key=int, value=str)
    key = 10
    value = "this"
    assert key.to_bytes((key.bit_length() + 7) // 8, "big") == rt.key_to_bytes(key)
    assert key == rt.key_from_bytes(key.to_bytes((key.bit_length() + 7) // 8, "big"))
    assert value.encode("utf-8") == rt.value_to_bytes(value)
    assert value == rt.value_to_bytes(value).decode("utf")


def test_bulk_load_items(tmpdir):
    rt = ResolutionTable(os.path.join(tmpdir, "lists/"), key=int, value=list)
    data = [[2, [1, 3]], [4, [5, 6]]]
    rt.bulk_load(iter_obj=data)
    rt.close()
    db = plyvel.DB(rt.path, create_if_missing=True)
    result = db.get(ResolutionTable.int_to_bytes(2))
    assert msgpack.unpackb(result) == [1, 3]
    db.close()
