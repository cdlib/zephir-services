
# CID Inquiry Workflow


    # combine record ocns and OCLC ocns, and dedup
    combined_ocns_list = flat_and_dedup_sort_list([ocns] + oclc_ocns_list)

    # Zephir lookup by OCNs in comma separated, single quoted strings
    # returns: list of cid and ocn returned from Zephir DB 
    cid_ocn_list = find_zephir_clusters_by_ocns(db_conn_str, combined_ocns_list)

    # create an object with the Zephir cluster lookup result
    zephir_clusters_result = ZephirClusterLookupResults(combined_ocns_list, cid_ocn_list) 

1. Prepare input OCNs in list of integers
   For example: [1, 6567842, 1000000000]

2. Lookup for OCNs clusters in the OCLC Concordance Table

   `get_clusters_by_ocns(ocns, primarydb_path="primary-lookup", clusterdb_path="cluster-lookup")`

    Input:
      * ocns: list of OCNs in integers. For example: [1, 6567842, 1000000000]
      * primarydb_path: full path to the OCN priamry LevelDB
      * clusterdb_path: full path to the OCN cluster LevelDB

    output: A set of OCN tuples, each representing a resolved OCLC OCNs cluster
    For example: {(1000000000,), (1, 6567842, 9987701, 53095235, 433981287)}

3. Compile the OCLC lookup results in the OclcLookupResult object with attributes of:

<pre>
        # OCNs in record: list of integers
        OclcLookupResult.inquiry_ocns = inquiry_ocns

        # OCNs in matched OCLC clusters: list of OCNs lists in integers
        OclcLookupResult.matched_ocns_clusters = list_of_ocns

        # number of matched OCLC clusters
        OclcLookupResult.num_of_matched_clusters = len(list_of_ocns)
</pre>
    While the list_of_ocns is a list converted from the set of OCN tuples from Step 2 output.

4. Construct an OCNs list for Zephir clusters lookup
   Combine the record ocns and OCLC ocns, and remove duplicates. 

5. Search Zephir clusters by ocns 

    `find_zephir_clusters_by_ocns(db_conn_str, combined_ocns_list)`
    Input:
      * db_conn_str: database connection string 
      * combined_ocns_list: list of OCNs in integer
    Returns: a list of cid and ocn tuples

6. Compile the Zephir lookup results in the ZephirClusterLookupResults object with attributes of:
<pre>
        # list of cid and ocn tuples
        ZephirClusterLookupResults.cid_ocn_list = cid_ocn_list

        # dict with key="cid", value=list of ocns
        ZephirClusterLookupResults.cid_ocn_clusters

        # number of cid clusters
        ZephirClusterLookupResults.num_of_matched_clusters

        # inquiry ocns
        ZephirClusterLookupResults.inquiry_ocns
</pre>
