import sys 

import msgpack
import plyvel

# convenience methods for converting ints to and from bytes
def int_to_bytes(inum):
    return inum.to_bytes((inum.bit_length() + 7) // 8, 'big')

def int_from_bytes(bnum):
    return int.from_bytes(bnum, 'big')


# given an oclc, get the master
def get_master_ocn(ocn):
    master = None
    try:
        mdb = plyvel.DB("master-lookup/", create_if_missing=True) 
        if mdb.get(int_to_bytes(ocn)):
            master = int_from_bytes(mdb.get(int_to_bytes(ocn)))
        print("ocn: {}, master: {}".format(ocn, master))
    finally:
        mdb.close()
    return master

# given a master, get a cluster of 1+
def get_cluster_by_master_ocn(master):
    cluster = [] 
    try:
        cdb = plyvel.DB("cluster-lookup/", create_if_missing=True)
        if cdb.get(int_to_bytes(master)):
            cluster = msgpack.unpackb(cdb.get(int_to_bytes(master)))
        else:
            cluster = [master]
        print("master: {}, cluster: {}".format(master, cluster))
    finally:
        cdb.close()
    return cluster


def get_cluster_by_ocn(ocn):
    master = get_master_ocn(ocn)
    if master is None:
        master = ocn
    cluster = get_cluster_by_master_ocn(master)
    return cluster

def lookup_ocns_from_oclc():
    """For a given oclc number, find all OCNs in the OCN cluster from the OCLC Concordance Table
    """
    if (len(sys.argv) > 1):
        ocn = int(sys.argv[1])
    else:
        ocn = 53095235

    cluster = get_cluster_by_ocn(ocn)

if __name__ == "__main__":
    lookup_ocns_from_oclc()

