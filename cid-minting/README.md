#CID Minting 

CID Minting includes a set to modules and functions to retrieve Zephir clusters based on the OCNs in the incoming record and any other OCNs that are in the OCLC record clusters.

## Modules

* oclc_lookup
* zephir_cluster_lookup
* cid_inquery
* cid_minting_store
 
### OCLC Lookup (oclc_lookup)
This module contains core functions to find OCNs relationships in the OCLC Concordance table:

* get_primary_ocn(ocn, db_path="primary-lookup"):
  Gets the primary oclc number for a given oclc number.

* get_ocns_cluster_by_primary_ocn(primary_ocn, db_path="cluster-lookup"):
  Gets all OCNs of an oclc cluster by a primary OCN.

* get_ocns_cluster_by_ocn(ocn, primarydb_path="primary-lookup", clusterdb_path="cluster-lookup"):
  Gets all OCNs of an oclc cluster by an OCN.

* get_clusters_by_ocns(ocns, primarydb_path="primary-lookup", clusterdb_path="cluster-lookup"):
  Finds the OCN clusters for a list of OCNs.

#### Excute the script
The `oclc_lookup.py` script can be executed from the command line. It takes a list of OCNs (space separated integers) and returns resolved OCNs clusters from the OCLC Concordance Table. 
```
pipenv run python oclc_lookup.py 1 123
```

### Zephir Cluster Lookup (zephir_cluster_lookup)
This module contains core functions to retrieve Zephir clusters:

* find_zephir_clusters_by_ocns(db_conn_str, ocns_list):
* find_zephir_clusters_by_cids(db_conn_str, cid_list):
* zephir_clusters_lookup(db_conn_str, ocns_list):
  Finds Zephir clusters by OCNs and returns Zephir clusters infomation such as cluster IDs, number of clusters and all OCNs in each cluster. 

### CID Inquiry (cid_inquiry)
The cid_inquiry module has one core funciton which is to find matched Zephir Clusters by a list of OCNs :
* cid_inquiry(ocns, db_conn_str, primary_db_path, cluster_db_path):
** Returns: a dict combining both OCLC lookup and Zephir lookup results:
<pre>
       "inquiry_ocns": input ocns, list of integers.
       "matched_oclc_clusters": OCNs in matched OCLC clusters, list of lists in integers.
       "num_of_matched_oclc_clusters": number of matched OCLC clusters.
       "inquiry_ocns_zephir": ocns list used to query Zephir DB.
       "cid_ocn_list": list of cid and ocn tuples from DB query.
       "cid_ocn_clusters": dict with key="cid", value=list of ocns in the cid cluster
       "num_of_matched_zephir_clusters": number of matched Zephir clusters.
       "min_cid": lowest CID among matched Zephir clusters
</pre>

#### CID Inquiry Workflow

* 1. Prepare input OCNs in list of integers. For example: [1, 6567842, 1000000000].
* 2. Lookup for OCNs clusters in the OCLC Concordance Table based on given OCNs.
* 3. Combine the input ocns and OCLC ocns, and remove duplicates. 
* 4. Search Zephir clusters by the combined ocns. 
* 5. Return compiled OCLC and Zephir lookup results

#### Execute the Script

The `cid_inquiry.py` script can be executed from the command line to find Zephir clusters by the OCNs.

Command line arguments:
* argv[1]: Server environemnt (Required). Can be dev, stg, or prd.
* argv[2]: List of OCNs (Required).
  Comma separated strings without spaces in between any two values.
  For example: 1,6567842,6758168,8727632

```
pipenv run python cid_inquiry.py dev 1,6567842,6758168,8727632
```

The script returns a dict combining both OCLC lookup and Zephir lookup results listed in the `cid_inquiry` function section. 

#### Shell Wrapper

The Shell script `run_cid_inquiry.sh` can be used to excute the Python `cid_inquiry.py` script. It checks the server environment (dev/stg/prd) and validtes input OCNs to make sure valid arguments are passed to the Python script. The script takes one argument which is a list of OCNs separated by comma with out spaces in between any two values. 

```
run_cid_inquiry.sh 1,6567842,6758168,8727632
```
### CID Minting Store (cid_minting_store)
This module has core functions for storing and finding a record's cluster ID assigend by the record prepartation process in the cid_minting_store table in the Zephir database. The cid_minting_store table only holds new CIDs that have not been reflected in the Zephir database due to delays in records loading. The CIDs in the cid_minting_store table will be used as an additonal resource for CID minting in the prepare process. 
