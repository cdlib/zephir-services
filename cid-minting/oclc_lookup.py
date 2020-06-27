import sys 

import msgpack
import plyvel

# convenience methods for converting ints to and from bytes
def int_to_bytes(inum):
    return inum.to_bytes((inum.bit_length() + 7) // 8, 'big')

def int_from_bytes(bnum):
    return int.from_bytes(bnum, 'big')


def get_primary_ocn(ocn):
    """Gets the primary oclc number for a given oclc number. 

    Retrieves the primary oclc number for a given oclc number (ocn) from LevelDB primary-lookup
    which contains the key/value pairs of an oclc number (key) and the resolved primary oclc number (value).
    Both key and value are integers.

    Args:
        ocn: An integer representing an oclc number

    Returns:
        An integer representing the primary oclc number of the given ocn;
        None if the given ocn has no matched key in the database.
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

def get_ocns_cluster_by_primary_ocn(primary):
    """Gets all OCNs of an oclc record by the primary OCN. 

    Retrieves the OCNs of an OCLC record identified by a primary OCN from the key/value LevelDB cluster-lookup.
    The key and value  are defined as:
        key: an integer representing a primary OCN
        value: list of integers containig [primary OCN + previous OCN(s) belong to the OCLC primary record].
    
    The cluster-lookup database only contains key/value pairs for OCLC records with previous OCN(s). For example:
        key=1, value=[1, 433981287, 6567842, 53095235, 9987701]
        key=4, value=[4, 518119215]

    When the given OCN is missing from the key of the cluster-lookup database, assume the resolved record 
    only contains a primary OCN and returns a list of the given OCN.

    Args:
        primary: An integer representing a primary OCN. 

    Returns:
        A list of integers representing the primary OCN + previous OCN(s) cluster the given OCN belongs. For example:
        when primary=1, return: [1, 433981287, 6567842, 53095235, 9987701]
        when primary=4, return: [4, 518119215]
        when primary=1000000000, return: [1000000000] (1000000000 in not in the key/value cluster-lookup db) 

        None if the given primary OCN is None.
    """
    cluster = None 
    if primary is None:
        return None
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


def get_ocns_cluster_by_ocn(ocn):
    """Gets all ONCs of an oclc record by an OCN 

    Retrieves the OCNs of an OCLC record:
        1. Finds the primary OCN of the given OCN;
        2. If found primary OCN, retrieves the OCNs cluster by the primary OCN;
        3. Returns the OCNs cluster if found; 
        4. Otherwise return None. 

    Args:
        ocn: An integer representing an OCN.

    Returns:
        A list of integers representing the primary OCN + previous OCN(s) cluster the given OCN belongs to. For example:
        when ocn=1, return: [1, 433981287, 6567842, 53095235, 9987701]
        when ocn=4, return: [4, 518119215]
        when ocn=1000000000, return: [1000000000]

        Returns None if the given OCN does not belong to any oclc cluster.
    """

    cluster = None 
    primary = get_primary_ocn(ocn)
    if primary:
        cluster = get_ocns_cluster_by_primary_ocn(primary)
    return cluster

def get_clusters_by_ocns(ocns):
    clusters = []
    for ocn in ocns:
        cluster = get_ocns_cluster_by_ocn(ocn)
        if cluster:
            clusters.append(cluster)
    # dedup
    print("clusters before dedup: {}".format(clusters))
    deduped_cluster = set(tuple(i) for i in clusters)
    print("dedup'ed cluster: {}".format(deduped_cluster))
    return deduped_cluster

def test(ocn):
    print("#### testing OCN={}".format(ocn))
    primary = get_primary_ocn(ocn)
    print("primary OCN: {}".format(primary))
    cluster = get_ocns_cluster_by_primary_ocn(primary)
    print("cluster by primary ({}): {}".format(primary, cluster))
    cluster = get_ocns_cluster_by_ocn(ocn)
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

    ocn=999999999 
    print("#### 9 digits, invalid ocn={}".format(ocn))
    test(ocn)

    ocn=1234567890123
    print("#### test a 13 digits, invalid ocn={}".format(ocn))
    test(ocn)

    ocns=[1,2, 1000000000]
    print("#### test list of ocns={}".format(ocns))
    test_ocns(ocns)

    ocns=[1, 1, 53095235, 2, 12345678901, 1000000000]
    print("#### test list of ocns={}".format(ocns))
    test_ocns(ocns)

    ocns=[1234567890]
    print("#### test list of ocns={}".format(ocns))
    test_ocns(ocns)

    ocns=[1234567890, 12345678901]
    print("#### test list of ocns={}".format(ocns))
    test_ocns(ocns)

    ocns=[]
    print("#### test list of ocns={}".format(ocns))
    test_ocns(ocns)

    #cluster = get_ocns_cluster_by_ocn(ocn)

if __name__ == "__main__":
    lookup_ocns_from_oclc()

