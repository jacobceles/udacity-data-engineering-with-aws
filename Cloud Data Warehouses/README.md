# Sparkify ETL Pipeline

## Overview

This project implements an ETL (Extract, Transform, Load) pipeline for Sparkify, a fictional music streaming service. The pipeline extracts data from JSON files stored in Amazon S3, stages it in Redshift, and transforms it into a set of dimensional tables for analysis.

## Project Structure

1. **create_table.py**: This script creates the fact and dimension tables for the star schema in Redshift. It connects to the Redshift database, drops existing tables if they exist, and creates new tables based on predefined schemas.

2. **etl.py**: This script loads data from S3 into staging tables on Redshift and then processes that data into analytics tables on Redshift. It also handles the transformation of data from staging tables to analytics tables.

3. **sql_queries.py**: This module contains SQL statements used in the `create_table.py` and `etl.py` scripts. It includes CREATE and INSERT statements for defining tables and loading data.

4. **README.md**: This document provides an overview of the project, instructions on how to run the Python scripts, and explanations of the files in the repository.

5. **pipeline.ipynb**: This Jupyter Notebook script serves as a pipeline executor. It runs the `create_tables.py` and `etl.py` scripts sequentially.

## Rubric Compliance

### Table Creation
- **Table Creation Script**: The `create_tables.py` script successfully connects to the Sparkify Redshift database, drops existing tables if they exist, and creates new tables as per the defined schemas.
- **Staging Tables Definition**: The `sql_queries.py` specifies all columns for songs and logs staging tables with correct data types and conditions.
- **Fact and Dimensional Tables**: The `sql_queries.py` specifies all columns for each of the five tables in the star schema with correct data types and conditions.

### ETL
- **ETL Script Execution**: The `etl.py` script runs without errors, connects to the Sparkify Redshift database, loads log_data and song_data into staging tables, and transforms them into the five tables.
- **Transformation Process**: INSERT statements are correctly written for each table, handling duplicate records where appropriate. Both staging tables are used to insert data into the songplays table.

### Code Quality
- **Documentation**: The `README.md` includes a summary of the project, instructions on running the Python scripts, and explanations of the repository files. Effective comments and docstrings are used throughout the project.
- **Modularity**: Scripts have an intuitive, easy-to-follow structure with code separated into logical functions. Variable and function naming follows the PEP8 style guidelines.

## Running the Pipeline

To run the ETL pipeline, follow these steps:

1. Set up an Amazon Redshift cluster and configure the connection details in `dwh.cfg`. To decrease the overall time, we can do eithero f the two:
<ol>
<li>If possible, use a dc2.large cluster with 8 nodes, or 
<li>Set `SONG_DATA='s3://udacity-dend/song-data/A/'` in `dwh.cfg`.  
</ol>
2. Execute the `pipeline.ipynb` script or run `create_tables.py` followed by `etl.py` in the terminal to create tables and load data into the Redshift database.
3. Verify the data was loaded by going into the Redshift cluster and running SELECT statements on the tables.