| Author | Major Update |
| --- | --- |
| Charlie Collett | 2018/011/26 |

# Bibliographic Export

## Description
The bibliographic export is an export of all HT ingested items, condensed into manifestation-level records.

### Who's it for?
* HT Rights and Access - to assess rights and access from bibliographic and holdings data.
* HT Bibliographic Indexing - to provide bibliographic information for the catalog, full-text search, and other functions.

### Why build it?
* To provide a current source of bibliographic and holdings data for every item ingested in HathiTrust.

## Requirements and Specifications

* Improving your spec
* Is this better.

### Functional Requirements
* Produce an export of ingested items in HathiTrust Digital Repository
  * Daily incremental export for all title-level bibliographic data and holdings potentially modified by Zephir.
  * Monthly complete export for all title-level bibliographic data and holdings.
* Export should include only one bibliographic record per manifestation.
* Export needs to be formatted to be compatible with downstream processes.
* Export needs to follow/retain specifications for HT MARC field naming and placement of operational fields.
* Export needs to create bibliographic records based on an agreed [methodology](BIBLIO-METHODOLOGY.md).

### Technical Requirements
* Exports only include records of items ingested into the HathiTrust repository.
* Incremental export includes records with same-day database update date.
* Full export includes all records.
* Export for full UTC day.
* Export daily for incremental export at minimum.
* Export monthly for full export at minimum.
* Export in MARC-in-JSON format.
