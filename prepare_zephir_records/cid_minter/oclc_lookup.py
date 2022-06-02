import sys
import os

import environs
import msgpack
import plyvel
import click

from lib.utils import get_configs_by_filename

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
        value: list of integers containing all previous OCNs of the OCLC cluster.

    Note:
    The cluster-lookup database only contains key/value pairs for OCLC clusters with previous OCN(s).
    Single OCLC clusters that do not have previous OCNs are not presented in the cluster-lookup DB.
    For example:
        key=1 (primary ocn), value=[433981287, 6567842, 53095235, 9987701]
        key=4 (primary ocn), value=[518119215]
        There is no key/value pair for ocn=1000000000 as there is no other ocns in this cluster.
    Args:
        primary_ocn: An integer representing a primary OCN.

    Returns:
        A list of integers representing an OCLC cluster with the primary and previous OCNs if there are any;
        None if the given OCN is None or 
        None if given OCN is not a primary OCN.

        For exampe:
            when primary_ocn=1000000000, return None
            when primary_ocn=1, return: [433981287, 6567842, 53095235, 9987701]
            when primary_ocn=4, return: [518119215]
            when primary_ocn=518119215, return: None
            when primary_ocn=1234567890, return: None
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
        when ocn=433981287, return: [1, 433981287, 6567842, 53095235, 9987701]
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

    Note: Returning set of tuples makes unit tests easier.
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

def lookup_ocns_from_oclc(ocns, primary_db_path, cluster_db_path ):
    """For a given list of OCNs find their associated OCN clusters from the OCLC Concordance Table
    Args:
        ocns: list of intergers representing OCNs
        primary_db_path: full path to the OCNs primary LevelDB
        cluster_db_path: full path to the OCNs cluster LevelDB
    Returns:
        A dict with:
        'inquiry_ocns': input ocns, list of integers.
        'matched_oclc_clusters': OCNs in matched OCLC clusters, list of integers lists
        'num_of_matched_oclc_clusters': number of matched OCLC clusters
    """

    # oclc lookup by a list of OCNs in integer
    # returns: A Set of tuples containing OCNs of resolved OCN clusters
    set_of_tuples = get_clusters_by_ocns(ocns, primary_db_path, cluster_db_path)

    # convert to a list of OCNs lists
    oclc_ocns_list = convert_set_to_list(set_of_tuples)

    # create an object with the OCLC lookup result
    oclc_lookup_result = {
        "inquiry_ocns": ocns,
        "matched_oclc_clusters": oclc_ocns_list,
        "num_of_matched_oclc_clusters": len(oclc_ocns_list),
    }
    return oclc_lookup_result

def tests():
    clusters_raw = {'1': [6567842, 9987701, 53095235, 433981287],
            '1000000000': None,
            '2': [9772597, 35597370, 60494959, 813305061, 823937796, 1087342349],
            '4': [518119215],
            '17216714': [535434196],
            }

    clusters = {'1': [6567842, 9987701, 53095235, 433981287, 1],
            '1000000000': [1000000000],
            '2': [2, 9772597, 35597370, 60494959, 813305061, 823937796, 1087342349],
            '4': [4, 518119215],
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
    print("should return raw clusters without primary ocn in the cluster-db")
    '''     when primary_ocn=1000000000, return None
            when primary_ocn=1, return: [433981287, 6567842, 53095235, 9987701]
            when primary_ocn=4, return: [518119215]
            when primary_ocn=518119215, return: None
            when primary_ocn=1234567890, return: None
    '''
    primary_ocn = 1000000000
    assert get_ocns_cluster_by_primary_ocn(primary_ocn) == None

    primary_ocn = 1
    cluster = get_ocns_cluster_by_primary_ocn(primary_ocn)
    print("cluster: {}".format(cluster))
    print("sorted:  {}".format(sorted(cluster)))
    assert sorted(cluster) == sorted(clusters_raw['1'])

    primary_ocn = 4 
    cluster = get_ocns_cluster_by_primary_ocn(primary_ocn)
    print("cluster: {}".format(cluster))
    print("sorted:  {}".format(sorted(cluster)))
    assert sorted(cluster) == sorted(clusters_raw['4'])

    primary_ocn = 518119215     # a previous ocn
    assert get_ocns_cluster_by_primary_ocn(primary_ocn) == None

    primary_ocn = 1234567890  # an invalid ocn
    assert get_ocns_cluster_by_primary_ocn(primary_ocn) == None

    primary_ocn = None
    assert get_ocns_cluster_by_primary_ocn(primary_ocn) == None
    print("**** finished testing get_ocns_cluster_by_primary_ocn(primary_ocn)")

    print("#### testing get_ocns_cluster_by_ocn: returning list of integers")
    print("should return clusters with the primary ocn")
    '''
        when ocn=1, return: [1, 433981287, 6567842, 53095235, 9987701]
        when ocn=6567842, return: [1, 433981287, 6567842, 53095235, 9987701]
        when ocn=4, return: [4, 518119215]
        when ocn=1000000000, return: [1000000000] (a cluster without previous OCNs)
        when ocn=1234567890, return: None (for an invalid ocn)
    '''
    ocn = 1
    cluster = get_ocns_cluster_by_ocn(ocn)
    print(cluster)
    assert sorted(get_ocns_cluster_by_ocn(ocn)) == sorted(clusters['1'])

    ocn = 6567842    # previous ocn
    assert sorted(get_ocns_cluster_by_ocn(ocn)) == sorted(clusters['1'])

    ocn = 4 
    assert sorted(get_ocns_cluster_by_ocn(ocn)) == sorted(clusters['4'])

    ocn = 1000000000
    assert get_ocns_cluster_by_ocn(ocn) == [1000000000]

    ocn = 1234567890 # invalid ocn
    assert get_ocns_cluster_by_ocn(ocn) == None

    ocn = None
    assert get_ocns_cluster_by_ocn(ocn) == None
    print("**** finished testing get_ocns_cluster_by_ocn")

    print("#### testing  get_clusters_by_ocns(ocns)")
    ocns=[1]    # resolve to cluster[1]
    clusters = get_clusters_by_ocns(ocns)
    print(clusters)
    print(set_1)
    assert clusters ^ set_1 == set()

    ocns=[1000000000, 1000000000]    #  resolve to cluster[1000000000]
    clusters = get_clusters_by_ocns(ocns)
    print(clusters)
    print(set_10)
    assert clusters ^ set_10 == set()

    ocns=[1, 1000000000]    #    resolve to 2 ocn clusters
    clusters = get_clusters_by_ocns(ocns)
    print(clusters)
    print(set_1)
    print(set_10)
    assert clusters ^ (set_1 | set_10) == set()

    ocns=[1, 1, 53095235, 2, 12345678901, 1000000000]
    clusters = get_clusters_by_ocns(ocns)
    print(clusters)
    print(set_1)
    print(set_2)
    assert clusters ^ (set_1 | set_2 | set_10) == set()

    ocns=[1234567890]
    assert get_clusters_by_ocns(ocns) == set()

    ocns=[1234567890, 12345678901]
    assert get_clusters_by_ocns(ocns) == set()

    ocns=[]
    assert get_clusters_by_ocns(ocns) == set()

@click.command()
@click.option('-t', '--test', is_flag=True, help="Will run a set of tests.")
@click.argument('ocns', nargs=-1)
def main(test, ocns):
    """For a given list of OCNs, find all resolved OCNs clusters from the OCLC Concordance Table.

    Provide the OCNs list in space separated integers, for example: 1 123.

    cmd: pipenv run python oclc_lookup.py 1 123

    returns: {(123, 18329830, 67524283), (1, 6567842, 9987701, 53095235, 433981287)}
    """
    if test:
        click.echo("Running tests ...")
        tests()
        exit(0)

    ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(ROOT_PATH, 'config')

    configs = get_configs_by_filename(CONFIG_PATH, "cid_minting")
    primary_db_path = configs["primary_db_path"]
    cluster_db_path = configs["cluster_db_path"]

    PRIMARY_DB_PATH = os.environ.get("OVERRIDE_PRIMARY_DB_PATH") or primary_db_path
    CLUSTER_DB_PATH = os.environ.get("OVERRIDE_CLUSTER_DB_PATH") or cluster_db_path

    ocns_list = list(int(ocn) for ocn in ocns)
    if ocns_list:
        clusters = get_clusters_by_ocns(ocns_list, PRIMARY_DB_PATH, CLUSTER_DB_PATH)
        click.echo(clusters)
        exit(0)
    else:
        ctx = click.get_current_context()
        click.echo(ctx.get_help())
        exit(1)

if __name__ == "__main__":
    main()
