## Zephir Resolve

Zephir resolve is a set of scripts and class abstractions to build identifier resolution tables for Zephir. For example, find the current (primary) OCN given an older OCN, or ind former OCNs for a given current OCN.

### ResolutionTable Class
This class is an abstraction for creating a LevelDB database of keys and values. It allows use of LevelDB without needing to work with the  conversion of types to bytes and back.

### Lookup Table Scripts

The following scripts help build LevelDB databases for use in Zephir.

#### Creating a primary lookup table.

1. Run the *create_primary_only_list.py* to filter out all rows except where a number resolves to itself (primary number) from the raw concordance.

2. Run *concat_files.py* to concatate the primary only list with the validated/pre-processed concordance file.

3. Run that ouptut to the *create_primary_table.py* to create a primary lookup table from the concatenated output.

Done! There should be a working LevelDB primary lookup table.

#### Creating a cluster lookup table.

1. Run the *create_cluster_file.py* on the validated/pre-processed concordance file to sort and position the data for laoding.

3. Run that ouptut to the *create_cluster_table.py* to create a cluster lookup table from the cluster file output

Done! There should be a working LevelDB cluster lookup table.
