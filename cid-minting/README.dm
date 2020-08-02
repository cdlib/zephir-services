# CID Inquiry

## Modules
* oclc_lookup
* zephir_cluster_lookup
* cid_inquery
* cid_minting_store
 
### OCLC Lookup (oclc_lookup)
Core functions to find OCNs relationships in the OCLC Concordance table:
* get_primary_ocn(ocn, db_path="primary-lookup"):
  Gets the primary oclc number for a given oclc number.

* get_ocns_cluster_by_primary_ocn(primary_ocn, db_path="cluster-lookup"):
  Gets all OCNs of an oclc cluster by a primary OCN.

* get_ocns_cluster_by_ocn(ocn, primarydb_path="primary-lookup", clusterdb_path="cluster-lookup"):
  Gets all OCNs of an oclc cluster by an OCN.

* get_clusters_by_ocns(ocns, primarydb_path="primary-lookup", clusterdb_path="cluster-lookup"):
  Finds the OCN clusters for a list of OCNs.

### Zephir Cluster Lookup (zephir_cluster_lookup)
Core functions to find Zephir clusters:
* find_zephir_clusters_by_ocns(db_conn_str, ocns_list):
* find_zephir_clusters_by_cids(db_conn_str, cid_list):
* zephir_clusters_lookup(db_conn_str, ocns_list):
  Finds Zephir clusters by OCNs and returns clusters' info including cluster IDs, number of clusters and all OCNs in each cluster. 

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

* 1. Prepare input OCNs in list of integers. For example: [1, 6567842, 1000000000]
* 2. Lookup for OCNs clusters in the OCLC Concordance Table
* 3. Combine the input ocns and OCLC ocns, and remove duplicates. 
* 4. Search Zephir clusters by the combined ocns 
* 5. Return compiled OCLC and Zephir lookup results

### CID Minting Store (cid_minting_store)

