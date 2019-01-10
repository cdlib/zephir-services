Author | Major Update
--- | --- | ---
Charlie Collett | 2018/08/20

# ZED Validate Logs

## Description
This application ensures ZED data events written to logs are well-formed, complies with schemas, and have been propagated through the ZED pipeline before archiving. The primary use case is to validate logs on a server after completion, but before archiving.

### Who's it for?
* ZED system - to insure data quality and propagation.
* Developers - to validate log formats in development.
* Analysts - a reference to log formats, current and historical.

### Why build it?
* Avoid bad data from being created, propagated.
* Prevent data loss of the ZED Events as it is transferred to the database.
* Keep a record of schemas for the data for future analysis

## Requirements and Specifications

### Functional Requirements
* Check event data produced is well-formed json. [1]
* Check for duplicate keys. [2]
* Require event data conform to expected jsonschema standards in general, and optionally by event. [3,4]
* Use a repository of schemas that can be used by developers and analysts, with accessible previous versions. [5]
* Check events not dropped by failing log shipping processes.[6]
* Prepare event data for archiving. [7]
* Allow only subset of validation features in needed. [8]

### Stories

#### 1. Application produces poorly formatted event logs
A new event has been adding to an application's ZED event stream log. Unfortunately, there is a bug and the output is breaking the serialization format. This will likely fail in the log shipping, and the event may fail to get into all the ZED systems and be accessible for querying. The validation routine should identify a poorly serialized event.

#### 2. Two or more events share the same key in the same log
A bug is causing events to be duplicated in the logs. This will result in double counting in aggregated reports for some of the ways we query ZED data. The validation routine should identify duplicate ids in the same log file.

#### 3. Event data does not conform to the general schema
A routine in a application has been refactored, and it's missing general ZED event 'subject' field. This data will have difficulty joining other events in queries. The validation routine should identify when events are not conforming to the general schema.

#### 4. Event data does not conform to a specific schema
A new event has been designed and implemented, but it's 'report' field is missing key elements that were specified in the data schema. This will baffle analysts later when trying to query against the schema, and may indicate not all the data expected is being recorded. The validation routine should identify when events are not conforming to their specific schema.

#### 4. Analyst needs to query an event that has changed schema over time.
An analyst needs to query the event logs for an event with multiple schema versions. The schema changed to accommodate new values in the 'report' field. The analyst must review the differences of the schemas over-time. They may need to validate older existing logs against the older schema.

#### 6. Data did not make it through the full ZED tracker pipeline
The database was down briefly for an update while events were being shipped through the ZED pipeline. As a result, some of the events were dropped by Fluentd. The validation routine should cross-check all events have made it through to the database.

#### 7. Which files have been validated are ready for archiving
The archive script is ready to archive validated files while skipping files that have not been validated. The validation routine should have a method to indicate log files that have been validated so other scripts can act accordingly.

#### 8. A programmer is validating during development and wants to skip cross-checks with production data.
A ZED data application is in mid-development, and a developer wants to make sure the script is creating valid ZED events without changing file names and cross-checking checking the database. The validation script should allow parameters to skip parts of the validation routine that are not applicable for all uses.

### Technical Requirements

* Shall be implemented as a Python command-line interface.
* Shall run locally on servers.
* Shall be instantiated by cron.
* Shall operate on logs created on previous days.
* Shall rename successfully audited files.
* Shall implement jsonschema validation against event json entries.
* Shall use environment-appropriate database for event cross-checking.
* Shall allow flags to skip validation steps.
* Shall generate ZED event logs with the topic 'ops'.
* Shall document usage with `--help` and test cases.
