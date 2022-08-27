# Songs and Songs Played ETL Pipeline
Extracts song data and Sparkify songs played logs from an S3 bucket, and transforms and loads into Redshift database for further analytics.
The schema created for loading contains a songs played fact table and dim tables for users, songs, artists and time songs were played.

## Usage
An AWS Redshift cluster with permission to read S3 buckets is required. Host ARN and database name, username and password is required in the `dwh.cfg`  

```python
python3 create_tables.py
```
Drops all the tables in database.  
Creates all tables needed for song play analytics.

```python
python3 etl.py
```
Copies datasets from S3 bucket which contains Sparkify logs data and song data to database into staging tables.
Data is then transformed to produce fact and dim tables.

## Repo Files

### create_tables.py
Creates the Sparkify database

### etl.py
Runs ETL pipeline

### sql_queries.py
Contains SQL queries to create, drop and insert into tables

### dwh.cfg
Configuration file used to connect to database and S3 bucket 

## Schema
The star schema was used in the final datamart. The design allows fast aggregation and simplified queries (less joins than a snowflake schema). 
