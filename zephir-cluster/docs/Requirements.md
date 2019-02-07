

# Zephir Clustering

## Description
Zephir clustering is a third-generation codebase to group HathiTrust digital items by bibliographic title. The algorithm determines which items belong together by bibliographic data and assigns them the same cluster identifier. This identifier is used to regroup these items for analysis and export into bibliographic-level MARC records.

### Who's it for?
* HathiTrust systems - to group HathiTrust items by bibliographic title for process and indexing, including rights.
* Analysts - to view and inspect items by bibliographic title.
* Users - to find items by bibliographic title.

### Why build it?
* Eliminate the labor-intensive path for reclustering records.
* Enable for experimental or alternate clustering to run in parallel
* Perform better de-duplication of bibliographic clusters.
* Assign all records (regular and shadow records) using the same process.
* Create a more consistent, and reproducible approach for matching on local and OCLC identifiers.

## Clustering Design and Architecture

### Proposal

The proposal is two part.

1) To move the CID assignments algorithm to after records are processed and loaded into the pairtree and database. As part of this architecture, the CID would be stored outside of the metadata record, independently in the database. As a separate process that is independently stored, records that have been updated can be re/assigned to clusters. Also, by holding off clustering till entire submission files are processed, the CID algorithm will have access to all related records at the same time which will prevent fragmentation.

2) As a later refinement, the algorithm will move from matching one record to a cluster, to reevaluating all related records and reclustering those related records as needed. This will enable self-healing when bad metadata clustered records together and has now been remediated. It also ensures clustering is independent from the timing of when certain records are loaded, and reproducible.

This can be done in parallel to the current algorithm so we can evaluate any changes that take place. This architecture will also allow us to propose and test alternate clustering algorithms.

### Assumptions
The working assumptions when clustering records we are:
* Clusters should be the same no matter what order the records are added/updated.
* The cluster that a shadowed record is in shouldn’t be treated differently than any other record in the cluster.
* Records should not have their cluster ids (CIDs) reassigned unnecessarily.

### Functional Requirements

### Stories

#### 1.

### Technical Requirements

### Specific Issues to Fix (from 2nd-gen clustering)
Clusters should be the same no matter what order the records are added/updated but:
* If the same record occurs in a file, the first occurrence is used. If the same record is used in two different files, the second is used.
* If files are prepared faster than they can be added to the database, records from different files that should be clustered together may be assigned to different clusters. This can happen because the prepare step doesn’t record it’s decisions between runs.
* If a record is the “glue” of two sets of records in a cluster, updating that record does not cause the rest of the cluster to be reevaluated. They might not belong together anymore.    
* A new record might potentially cause a clusters to merge with another. Currently this only works if all of the records in the cluster get reprocessed. Unprepared records remain in the old cluster.

The cluster that a shadowed record is in shouldn’t be treated differently than any other record in the cluster but:
* Currently, the second principle is violated because shadowing records do not have their cluster id’s reevaluated, if the shadowed record is reprocessed.
