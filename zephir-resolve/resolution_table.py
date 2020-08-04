import os

import csv
import msgpack
import plyvel


class ResolutionTable:
    def __init__(self, path=".", key=None, value=None):
        self.path = os.path.join(path)
        self._db = plyvel.DB(self.path, create_if_missing=True)

        self._packer = msgpack.Packer()

        self.key = key
        self.value = value
        self.key_to_bytes = self.to_bytes_func(self.key)
        self.value_to_bytes = self.to_bytes_func(self.value)
        self.key_from_bytes = self.from_bytes_func(self.key)
        self.value_from_bytes = self.from_bytes_func(self.value)

    def close(self):
        self._db.close()

    def to_bytes_func(self, dtype=None):
        return {
            int: lambda value: self.int_to_bytes(value),
            str: lambda value: value.encode("utf-8"),
        }.get(dtype, lambda value: self._packer.pack(value))

    def from_bytes_func(self, dtype=None):
        return {
            int: lambda value: self.int_from_bytes(value),
            str: lambda value: value.decode("utf-8"),
        }.get(dtype, lambda value: msgpack.unpackb(value))

    def put(self, key, value):
        return self._db.put(self.key_to_bytes(key), self.value_to_bytes(value))

    def get(self, key):
        return self.value_from_bytes(self._db.get(self.key_to_bytes(key)))

    def bulk_load(self, iter_obj=[]):
        for item in iter_obj:
            self.put(item[0], item[1])

    def convert_dtypes_from_csv(self, csvreader):
        for row in csvreader:
            yield [self.key(row[0]), self.value(row[1])]

    @staticmethod
    def int_to_bytes(inum):
        return inum.to_bytes((inum.bit_length() + 7) // 8, "big")

    @staticmethod
    def int_from_bytes(bnum):
        return int.from_bytes(bnum, "big")
