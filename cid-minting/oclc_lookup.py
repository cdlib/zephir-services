import sys

import msgpack
import plyvel

class OclcLookupResult: 
    def __init__(self, inquiry_ocns, list_of_ocns):
        # OCNs in record: list of integers
        self.inquiry_ocns = inquiry_ocns

        # OCNs in matched OCLC clusters in String: comma separated, single quoted OCNs
        self.matched_ocns = lists_to_str(list_of_ocns)

        # OCNs in matched OCLC clusters: list of OCNs lists in integers
        self.matched_ocns_clusters = list_of_ocns

        # number of matched OCLC clusters
        self.num_of_matched_clusters = len(list_of_ocns)

# convenience methods for converting ints to and from bytes
def int_to_bytes(inum):
    return inum.to_bytes((inum.bit_length() + 7) // 8, 'big')

def int_from_bytes(bnum):
    return int.from_bytes(bnum, 'big')

def get_primary_ocn(ocn, db_path="primary-lookup"):
    """Gets the primary oclc number for a given oclc number.


    Retrieves the primary oclc number for a given oclc number (ocn) from LevelDB primary-lookup
    which contains the key/value pairs of an oclc number (key) and the resolved primary oclc number (value).
    Both key and value are integers.

    Args:
        ocn: An integer representing an oclc number.

    Returns:
        An integer representing the primary oclc number for the given ocn;
        None if the given ocn has no matched key in the primary-lookup database.
    """
    primary = None
    if ocn:
        try:
            mdb = plyvel.DB(db_path, create_if_missing=True)
            key = int_to_bytes(ocn)
            if mdb.get(key):
                primary = int_from_bytes(mdb.get(key))
        finally:
            mdb.close()
    return primary

def get_ocns_cluster_by_primary_ocn(primary_ocn, db_path="cluster-lookup"):
    """Gets all OCNs of an oclc cluster by a primary OCN.

    Retrieves the OCNs of an OCLC cluster from the LevelDB cluster-lookup with key and value defined as:
        key: an integer representing a primary OCN.
        value: list of integers containig the primary and previous OCNs of the OCLC cluster.

    Note:
    The cluster-lookup database only contains key/value pairs for OCLC clusters with previous OCN(s).
    This means the value fields are lists with 2 or more items containing the primary OCN and one or more previous OCNs.
    For example:
        key=1 (primary ocn), value=[1, 433981287, 6567842, 53095235, 9987701]
        key=4 (primary ocn), value=[4, 518119215]

    Args:
        primary_ocn: An integer representing a primary OCN.

    Returns:
        A list of integers representing an OCLC cluster with the primary and previous OCNs;
        None if the given OCN is None or is missing from the key/primary OCN of the cluster-lookup database.

        For exampe:
            when primary_ocn=1, return: [1, 433981287, 6567842, 53095235, 9987701]
            when primary_ocn=4, return: [4, 518119215]
            when primary_ocn=1000000000, return: None
                (1000000000 is a primary ocn for an OCN cluster without previous OCNs. It is not in the cluster-lookup db)
            when primary_ocn=1234567890, return: None
                (1234567890 is an invalid ocn and is not in the key/value cluster-looup db)
    """
    cluster = None
    if primary_ocn:
        try:
            cdb = plyvel.DB(db_path, create_if_missing=True)
            key = int_to_bytes(primary_ocn)
            if cdb.get(key):
                cluster = msgpack.unpackb(cdb.get(key))
        finally:
            cdb.close()
    return cluster


def get_ocns_cluster_by_ocn(ocn, primarydb_path="primary-lookup", clusterdb_path="cluster-lookup"):
    """Gets all OCNs of an oclc cluster by an OCN.

    Retrieves the OCNs of an OCLC cluster by a given OCN:
        1. Finds the primary OCN of the given OCN from the primary-lookup LevelDB;
        2. If found the primary OCN, retrieves the OCNs cluster by the primary OCN from the cluster-lookup LevelDB;
        3. If no cluster was found, creates a cluster contains only the primary OCN;

    Args:
        ocn: An integer representing an OCN.

    Returns:
        A list of integers representing an OCN cluster the given OCN belongs to in the format of [primary_OCN, previous_OCN(s)];
        None if the given OCN does not belong to any oclc cluster.

        For example:
        when ocn=1, return: [1, 433981287, 6567842, 53095235, 9987701]
        when ocn=4, return: [4, 518119215]
        when ocn=1000000000, return: [1000000000] (a cluster without previous OCNs)
        when ocn=1234567890, return: None (for an invalid ocn)
    """

    cluster = None
    primary = get_primary_ocn(ocn, primarydb_path)
    if primary:
        cluster = get_ocns_cluster_by_primary_ocn(primary, clusterdb_path)
        if cluster:
            cluster.append(primary)
        else:
            cluster = [primary]
    return cluster

def get_clusters_by_ocns(ocns, primarydb_path="primary-lookup", clusterdb_path="cluster-lookup"):
    """Finds the OCN clusters for a list of OCNs.

    Finds the OCN cluster each given OCN belongs to.
    Returns clusters found with duplications removed.

    Args:
        ocns: A list of integers representing OCNs.

    Returns:
        A Set of tuples containing OCNs of resolved OCN clusters;
        An empty set when there is no resolved OCN clusters.

    For example:
    When incoming ocns = [1, 433981287, 1000000000, 1234567890] where
        ocn=1 resolves to OCN cluster [1, 433981287, 6567842, 53095235, 9987701]
        ocn=433981287 also resolves to OCN cluster [1, 433981287, 6567842, 53095235, 9987701]
        ocn=1000000000 resolves to OCN cluster [1000000000]
        ocn=1234567890 resolves to None
    Returns a set: {(1000000000,), (1, 433981287, 6567842, 53095235, 9987701)}

    When incoming ocns only contains an invalid OCN(s), an empty set() will be returned.

    """
    clusters = []
    for ocn in ocns:
        cluster = get_ocns_cluster_by_ocn(ocn, primarydb_path, clusterdb_path)
        if cluster:
            clusters.append(cluster)

    # dedup
    deduped_cluster = set([tuple(sorted(i)) for i in clusters])
    return deduped_cluster

def convert_set_to_list(set_of_tuples):
    list_of_tuples = list(set_of_tuples)
    list_of_lists = [list(a_tuple) for a_tuple in list_of_tuples]
    return list_of_lists

def lists_to_str(list_of_lists):
    ocns = "" 
    for a_list in list_of_lists:
        for item in a_list:
            if ocns:
                ocns += ", '" + str(item) + "'"
            else:
                ocns = "'" + str(item) + "'"
    return ocns

def test(ocn):
    print(".... testing OCN={}".format(ocn))
    primary = get_primary_ocn(ocn)
    print("primary OCN: {}".format(primary))
    cluster = get_ocns_cluster_by_primary_ocn(primary)
    print("cluster by primary ({}): {}".format(primary, cluster))
    cluster = get_ocns_cluster_by_ocn(ocn)
    print("cluster by ocn ({}): {}".format(ocn, cluster))

def test_ocns(ocns):
    print(".... testing OCN={}".format(ocns))
    clusters = get_clusters_by_ocns(ocns)
    print("clusters by ocns ({}):".format(ocns))
    print("clusters: {}".format(clusters))

def lookup_ocns_from_oclc():
    """For a given oclc number, find all OCNs in the OCN cluster from the OCLC Concordance Table
    """
    if (len(sys.argv) > 1):
        ocn = int(sys.argv[1])
    else:
        ocn = None

    clusters = {'1': [6567842, 9987701, 53095235, 433981287, 1],
            '1000000000': [1000000000],
            '2': [2, 9772597, 35597370, 60494959, 813305061, 823937796, 1087342349],
            '17216714': [17216714, 535434196],
            }

    set_1 = set([tuple(sorted(clusters['1']))])
    set_10 = set([tuple(sorted(clusters['1000000000']))])
    set_2 = set([tuple(sorted(clusters['2']))])

    print("#### testing get_primary_ocn(ocn)")
    ocn = 1000000000
    assert get_primary_ocn(ocn) == 1000000000

    ocn = 1
    assert get_primary_ocn(ocn) == 1

    ocn = 6567842
    assert get_primary_ocn(ocn) == 1

    ocn = 1234567890
    assert get_primary_ocn(ocn) == None

    ocn = None
    assert get_primary_ocn(ocn) == None
    print("**** finished testing get_primary_ocn")

    print("#### testing get_ocns_cluster_by_primary_ocn(primary_ocn)")

    primary_ocn = 1000000000
    assert get_ocns_cluster_by_primary_ocn(primary_ocn) == None

    primary_ocn = 1
    assert get_ocns_cluster_by_primary_ocn(primary_ocn).sort() == clusters['1'].sort()

    primary_ocn = 6567842     # a previous ocn
    assert get_ocns_cluster_by_primary_ocn(primary_ocn) == None

    primary_ocn = 1234567890  # an invalid ocn
    assert get_ocns_cluster_by_primary_ocn(primary_ocn) == None

    primary_ocn = None
    assert get_ocns_cluster_by_primary_ocn(primary_ocn) == None
    print("**** finished testing get_ocns_cluster_by_primary_ocn(primary_ocn)")

    print("#### testing get_ocns_cluster_by_ocn: returning list of integers")
    ocn = 1000000000
    assert get_ocns_cluster_by_ocn(ocn) == clusters['1000000000']

    ocn = 1
    assert get_ocns_cluster_by_ocn(ocn).sort() == clusters['1'].sort()

    ocn = 6567842    # previous ocn
    assert get_ocns_cluster_by_ocn(ocn).sort() == clusters['1'].sort()

    ocn = 1234567890 # invalid ocn
    assert get_ocns_cluster_by_ocn(ocn) == None

    ocn = None
    assert get_ocns_cluster_by_ocn(ocn) == None
    print("**** finished testing get_ocns_cluster_by_ocn")

    print("#### testing  get_clusters_by_ocns(ocns)")
    ocns=[1]    # resolve to cluster[1]
    assert get_clusters_by_ocns(ocns) ^ set_1 == set()
    print(get_clusters_by_ocns([1]))

    ocns=[1000000000, 1000000000]    #  resolve to cluster[1000000000]
    assert get_clusters_by_ocns(ocns) ^ set_10 == set()

    ocns=[1, 1000000000]    #    resolve to 2 ocn clusters
    clusters = get_clusters_by_ocns(ocns)
    assert get_clusters_by_ocns(ocns) ^ (set_1 | set_10) == set()

    ocns=[1, 1, 53095235, 2, 12345678901, 1000000000]
    assert get_clusters_by_ocns(ocns) ^ (set_1 | set_2 | set_10) == set()

    ocns=[1234567890]
    assert get_clusters_by_ocns(ocns) == set()

    ocns=[1234567890, 12345678901]
    assert get_clusters_by_ocns(ocns) == set()

    ocns=[]
    assert get_clusters_by_ocns(ocns) == set()

if __name__ == "__main__":
    lookup_ocns_from_oclc()
