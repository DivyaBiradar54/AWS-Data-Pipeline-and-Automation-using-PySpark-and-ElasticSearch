=========
Overview:
=========

The goal of this project is to construct a data pipeline in AWS Cloud that will involve the integration of several AWS services S3, EMR, EC2, Step Function, and OpenSearch. The pipeline ingests data from multiple sources, performs necessary transformations using PySpark, and writes the final transformed data to the target systems: S3 and Elasticsearch.

========================
Data Pipeline Components:
=========================
The pipeline integrates three data sources and processes them using PySpark jobs. 

Data Sources:
-------------
S3: One of the sources for raw data.
Snowflake: A cloud-based data warehouse.
Web API: Extracts data from an external source.

==============
PySpark Jobs:
==============
Extraction Jobs: There are three PySpark jobs designed to extract data from the above three sources.
Transformation Jobs: Minimal transformations like filtering, aggregation, and flattening complex data structures are performed during the extraction phase.
Master PySpark Job: This job aggregates the intermediate results from the extraction jobs, performs all business-required transformations, and writes the final data to AWS S3 and Elasticsearch.
Target Systems:
S3: Intermediate and final data are stored here for downstream consumption.
Elasticsearch: The final data is indexed in Elasticsearch for searching and further transformations by other teams.

=================
Pipeline Workflow
=================
Data is extracted from S3, Snowflake, and the Web API using three PySpark jobs.
The extracted data is minimally transformed and written to intermediate AWS S3 locations.
A master PySpark job collects the intermediate data, applies final business-specific transformations, and writes the results to both AWS S3 and Elasticsearch.
Downstream teams consume this data for further processing and analytics.
Orchestration and Scheduling
Step Functions: We automate the entire workflow using AWS Step Functions. This includes provisioning EMR clusters to run PySpark jobs and terminating the clusters after completion.
EventBridge: AWS EventBridge is used to schedule the Step Functions, ensuring that the jobs are executed at 8 AM Eastern Time every day.

===========
How to Run:
===========

Ensure that the necessary data sources (S3, Snowflake, Web API) are properly configured.
The Step Function script should be deployed to AWS for orchestrating the EMR clusters and PySpark jobs.
Use AWS EventBridge to schedule the Step Functions to run at the desired time.
Monitor the workflow execution through the AWS Console.
Job Scheduling
The data pipeline runs daily at 8:00 AM Eastern Time, triggered by AWS EventBridge.
Downstream Integration
