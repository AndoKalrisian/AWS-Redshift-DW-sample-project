# Project: AWS Redshift Data Warehouse

## Purpose

To create an analytics data warehouse for the analytics team as Sparkify so they can find insights into what songs their users are listening to.

## Database schema

![alt text](/star_schema.png "Star Schema")

This schema was used to optimize queries about what songs are being played.  All the infomation needed for these queries can be found by querying the fact table.  For other queries, all other tables can be accessed by using only one JOIN.

## ETL Pipeline

The ETL pipeline used is as follows:
1. Extract using SQL COPY command from the 'songs' JSON data stored in a S3 Bucket and stage the songs in an Amazon Redshift table. 
2. Extract using SQL COPY command from the 'logs' JSON data stored in a S3 Bucket and stage the events in an Amazon Redshift table. 
3. Transform the data in the redshift staged song and events tables into a 'songplays' fact table shown in the database schema.
4. Transform the data in the redshift staged events and songs table into the users, songs, artists and time dimension tables shown in the database schema.

## How to use (Windows Powershell)

You will need to create you own credentials file that has your credentials in the following format:

Filename: 'redshift/aws_do_not_share.cfg'

[AWS]  
KEY=YOUR_KEY  
SECRET=YOUR_SECRET  
  
##### Create a redshift cluster:

```py redshift/create_redshift_cluster.py```

##### Create database tables:

```py create_tables.py```

##### Run ETL process:

```py etl.py```

##### Test out database.  Some test queries can be found in 'test.ipynb'.

##### Remove redshift cluster and roles associated with it:

```py redshift/remove_cluster_and_roles.py```


## Project Context

### Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

### Project Description
In this project, you'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.
