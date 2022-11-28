import sys
import os

import environs
import msgpack
import plyvel
import click

from lib.utils import get_configs_by_filename
from cid_minter.oclc_lookup import get_primary_ocn
from cid_minter.oclc_lookup import get_ocns_cluster_by_ocn
from cid_minter.oclc_lookup import get_ocns_cluster_by_primary_ocn
from cid_minter.oclc_lookup import get_clusters_by_ocns

def tests(primary_db_path, cluster_db_path):
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

    print("#### testing get_primary_ocn(ocn, primary_db_path)")
    ocn = 1000000000
    assert get_primary_ocn(ocn, primary_db_path) == 1000000000

    ocn = 1
    assert get_primary_ocn(ocn, primary_db_path) == 1

    ocn = 6567842
    assert get_primary_ocn(ocn, primary_db_path) == 1

    ocn = 1234567890
    result = get_primary_ocn(ocn, primary_db_path)
    print(f"incoming ocn={ocn} - primary ocn: {result}")
    assert get_primary_ocn(ocn, primary_db_path) == 1234567890

    ocn = None
    assert get_primary_ocn(ocn, primary_db_path) == None
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
    assert get_ocns_cluster_by_primary_ocn(primary_ocn, cluster_db_path) == None

    primary_ocn = 1
    cluster = get_ocns_cluster_by_primary_ocn(primary_ocn, cluster_db_path)
    print("cluster: {}".format(cluster))
    print("sorted:  {}".format(sorted(cluster)))
    assert sorted(cluster) == sorted(clusters_raw['1'])

    primary_ocn = 4 
    cluster = get_ocns_cluster_by_primary_ocn(primary_ocn, cluster_db_path)
    print("cluster: {}".format(cluster))
    print("sorted:  {}".format(sorted(cluster)))
    assert sorted(cluster) == sorted(clusters_raw['4'])

    primary_ocn = 518119215     # a previous ocn
    assert get_ocns_cluster_by_primary_ocn(primary_ocn, cluster_db_path) == None

    primary_ocn = 1234567890  # valid ocn
    result= get_ocns_cluster_by_primary_ocn(primary_ocn, cluster_db_path)
    print(f"cluster for ocn={primary_ocn}: {result}")
    assert get_ocns_cluster_by_primary_ocn(primary_ocn, cluster_db_path) == None

    primary_ocn = None
    assert get_ocns_cluster_by_primary_ocn(primary_ocn, cluster_db_path) == None
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
    cluster = get_ocns_cluster_by_ocn(ocn, primary_db_path, cluster_db_path)
    print(cluster)
    assert sorted(get_ocns_cluster_by_ocn(ocn, primary_db_path, cluster_db_path)) == sorted(clusters['1'])

    ocn = 6567842    # previous ocn
    assert sorted(get_ocns_cluster_by_ocn(ocn, primary_db_path, cluster_db_path)) == sorted(clusters['1'])

    ocn = 4 
    assert sorted(get_ocns_cluster_by_ocn(ocn, primary_db_path, cluster_db_path)) == sorted(clusters['4'])

    ocn = 1000000000
    assert get_ocns_cluster_by_ocn(ocn, primary_db_path, cluster_db_path) == [1000000000]

    ocn = 1234567890 # valid ocn
    result= get_ocns_cluster_by_ocn(ocn, primary_db_path, cluster_db_path)
    assert get_ocns_cluster_by_ocn(ocn, primary_db_path, cluster_db_path) == [1234567890]

    ocn = None
    assert get_ocns_cluster_by_ocn(ocn, primary_db_path, cluster_db_path) == None
    print("**** finished testing get_ocns_cluster_by_ocn")

    print("#### testing  get_clusters_by_ocns(ocns, primary_db_path, cluster_db_path)")
    ocns=[1]    # resolve to cluster[1]
    clusters = get_clusters_by_ocns(ocns, primary_db_path, cluster_db_path)
    print(clusters)
    print(set_1)
    assert clusters ^ set_1 == set()

    ocns=[1000000000, 1000000000]    #  resolve to cluster[1000000000]
    clusters = get_clusters_by_ocns(ocns, primary_db_path, cluster_db_path)
    print(clusters)
    print(set_10)
    assert clusters ^ set_10 == set()

    ocns=[1, 1000000000]    #    resolve to 2 ocn clusters
    clusters = get_clusters_by_ocns(ocns, primary_db_path, cluster_db_path)
    print(clusters)
    print(set_1)
    print(set_10)
    assert clusters ^ (set_1 | set_10) == set()

    ocns=[1, 1, 53095235, 2, 12345678901, 1000000000]
    clusters = get_clusters_by_ocns(ocns, primary_db_path, cluster_db_path)
    print(clusters)
    print(set_1)
    print(set_2)
    assert clusters ^ (set_1 | set_2 | set_10) == set()

    ocns=[1234567890]
    result= get_clusters_by_ocns(ocns, primary_db_path, cluster_db_path)
    print(result)
    assert get_clusters_by_ocns(ocns, primary_db_path, cluster_db_path) == {(1234567890,)}

    ocns=[1234567890, 12345678901]
    assert get_clusters_by_ocns(ocns, primary_db_path, cluster_db_path) == {(1234567890,)}

    ocns=[]
    assert get_clusters_by_ocns(ocns, primary_db_path, cluster_db_path) == set()

@click.command()
@click.option('-t', '--test', is_flag=True, help="Will run a set of tests.")
@click.argument('ocns', nargs=-1)
def main(test, ocns):
    """For a given list of OCNs, find all resolved OCNs clusters from the OCLC Concordance Table.

    Provide the OCNs list in space separated integers, for example: 1 123.

    cmd: pipenv run python oclc_lookup.py 1 123

    returns: {(123, 18329830, 67524283), (1, 6567842, 9987701, 53095235, 433981287)}
    """

    ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(ROOT_PATH, 'config')

    configs = get_configs_by_filename(CONFIG_PATH, "cid_minting")
    primary_db_path = configs["primary_db_path"]
    cluster_db_path = configs["cluster_db_path"]


    if test:
        click.echo("Running tests ...")
        tests(primary_db_path, cluster_db_path)
        exit(0)

    ocns_list = list(int(ocn) for ocn in ocns)
    if ocns_list:
        clusters = get_clusters_by_ocns(ocns_list, primary_db_path, cluster_db_path)
        click.echo(clusters)
        exit(0)
    else:
        ctx = click.get_current_context()
        click.echo(ctx.get_help())
        exit(1)

if __name__ == "__main__":
    main()
