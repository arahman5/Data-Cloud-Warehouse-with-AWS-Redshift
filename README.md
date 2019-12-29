# Data-lake-with-Spark
This repository contains the code of a ETL Pipeline that loads data from S3 buckets to staging tables on AWS Redshift and executes SQL statements that create the analytics tables from these staging tables.

## Project Description

A startup wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. Their analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As a data engineer I built an ETL pipeline that extracts the JSON logs from S3 buckets, stages them in AWS Redshift, and transforms data into a set of facts and dimensional tables for the analytics team to continue finding insights in what songs the users are listening to.

## Project Structure

* `data` -  folder containing all the JSON logs that contain data about user activity on app and metadata about songs.
* `sql_queries.py` - contains all sql queries that are used to Create, Insert data and Drop tables.
* `create_tables.py` - Run this file to reset tables before each time you run your ETL script.
* `dwh.cfg` -  Configuration file containing information about S3 buckets, Redshift Cluster and AWS Credential.
* `etl.py` -reads and processes all JSON logs and loads the data into the tables.
* `README.md` - Provides a summary of the project and discussions on the data modelling.
* `Resource` - Folder containing images that were used in the README.

## Choice of Database

The reason that I chose a relational database management system for this project are as follows:

* The volume of data the startup is dealing with is quite a small dataset.
* The data from the music streaming app is structured as we know how the key-value pairs are stored inside the JSON logs and what are their data types. 
* Easier to change to business requirements and flexibility in queries as the analytics team would want to perform ad-hoc queries to get a better understanding of the songs that different users are listening to.
* The analytics team would want to be able to do aggregations and analytics on the data.
* The ability to do JOINs would be very useful here due to the way data is getting logged in JSON files. Please see Entity relationship diagram below. Even though JOINs are slow, due to the small size of the dataset this shouldn't be a problem.

## Entity Relationship Diagram (ERD)

![Image](https://github.com/arahman5/Data-Cloud-Warehouse-with-AWS-Redshift/blob/master/resource/ERD.PNG)

The above represents the entity relationship diagram which shows how the different keys within the JSON logs can be connected together in a star schema. **songplays** is the fact table showing foreign keys that connect this table to all the other dimension tables. **users, time, songs, artists** are all dimension tables, each containing a primary key unique to each table. A star schema was chosen for this database design because of the following reasons:

* Star schema supports denormalization of the data, which would be quite useful in this analytics case as this will allow the analytics team to execute simple queries and fast aggregations on the data. 
* Star schema supports one to one mapping, which is easy to implement and works very well in this case due to the small number of keys within the JSON logs. 

## ETL Pipeline

Please see list of key steps that happens in the ETL Pipeline below:

* The logs for song metadata and event data about user activity in the app are both read from S3 and all the columns that are present in these two logs are loaded into the staging tables **staging_events** and **staging_songs** respectively in Redshift.
* All the facts and dimension tables mentioned in the above section are then created from these staging tables.


## Running the scripts

### Udacity Workspace

Firstly, download the contents of this repo in your Udacity workspace and then fill in the `dwh.cfg` file with your AWS IAM Role information and your Cluster information. After that, execute the below command in the terminals in order:

```python
python create_tables.py
```

```python
python etl.py
```

