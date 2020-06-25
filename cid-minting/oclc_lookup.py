import sys 

import msgpack
import plyvel

# convenience methods for converting ints to and from bytes
def int_to_bytes(inum):
    return inum.to_bytes((inum.bit_length() + 7) // 8, 'big')

def int_from_bytes(bnum):
    return int.from_bytes(bnum, 'big')


# given an ocn, get the primary ocn 
# return None if not find
def get_primary_ocn(ocn):
    """Gets the primary oclc number from the primary-lookup db

    Retrieves the primary oclc number for a given oclc number (ocn) from LevelDB primary-lookup
    which contains the key/value pairs of oclc number (key) and the primary oclc number (value)

    Args:
        ocn: An oclc number

    Returns:
        The primary oclc number of the given ocn;
        Or None if the goven ocn has no matched entry in the database 
    """
    primary = None
    try:
        mdb = plyvel.DB("primary-lookup/", create_if_missing=True) 
        key = int_to_bytes(ocn)
        if mdb.get(key):
            primary = int_from_bytes(mdb.get(key))
        #print("ocn: {}, primary: {}".format(ocn, primary))
    finally:
        mdb.close()
    return primary

# given a primary ocn, return the cluster of ocns from the cluster-lookup db 
# which contains clusterw with more than one ocns;
# if not found, retrun a cluster of it self (the primary ocn)
def get_cluster_by_primary_ocn(primary):
    cluster = [] 
    if primary is None:
        return cluster
    try:
        cdb = plyvel.DB("cluster-lookup/", create_if_missing=True)
        key = int_to_bytes(primary)
        if cdb.get(key):
            val=cdb.get(key)
            cluster = msgpack.unpackb(cdb.get(key))
        else:
            cluster = [primary]
        #print("primary: {}, cluster: {}".format(primary, cluster))
    finally:
        cdb.close()
    return cluster


def get_cluster_by_ocn(ocn):
    cluster = []
    primary = get_primary_ocn(ocn)
    if primary:
        cluster = get_cluster_by_primary_ocn(primary)
    return cluster

def get_clusters_by_ocns(ocns):
    clusters = []
    for ocn in ocns:
        cluster = get_cluster_by_ocn(ocn)
        clusters.append(cluster)
    return clusters

def test(ocn):
    print("#### testing OCN={}".format(ocn))
    primary = get_primary_ocn(ocn)
    print("primary OCN: {}".format(primary))
    cluster = get_cluster_by_primary_ocn(primary)
    print("cluster by primary ({}): {}".format(primary, cluster))
    cluster = get_cluster_by_ocn(ocn)
    print("cluster by ocn ({}): {}".format(ocn, cluster))

def test_ocns(ocns):
    print("#### testing OCN={}".format(ocns))
    clusters = get_clusters_by_ocns(ocns)
    print("clusters by ocns ({}): {}".format(ocns, clusters))

def lookup_ocns_from_oclc():
    """For a given oclc number, find all OCNs in the OCN cluster from the OCLC Concordance Table
    """
    if (len(sys.argv) > 1):
        ocn = int(sys.argv[1])
    else:
        ocn = 53095235

    ocn=1
    print("#### test primary ocn={}".format(ocn))
    test(ocn)

    ocn=1000000000
    print("#### test primary 10 digits ocn={}".format(ocn))
    test(ocn)

    ocn=53095235
    print("#### test previous ocn={}".format(ocn))
    test(ocn)

    ocn=12345678901 
    print("#### test a 11 digits (OK), invalid ocn={}".format(ocn))
    test(ocn)

    ocn=1234567890123
    print("#### test a 13 digits (OK), invalid ocn={}".format(ocn))
    test(ocn)

    ocns=[1,2, 1000000000]
    print("test list of ocns={}".format(ocns))
    test_ocns(ocns)

    #cluster = get_cluster_by_ocn(ocn)

if __name__ == "__main__":
    lookup_ocns_from_oclc()

