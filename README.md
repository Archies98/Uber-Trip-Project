## Summary

The project is a simulation of a Data Engineering workflow starting with ingestion, building an ETL pipeline, loading the data to a warehouse and finally creating a dashboard using the loaded data. 
As an aside, the data model was created before starting with the implementation of the pipeline.

## Tools

Programming Language: Python\
Data Lake: AWS S3\
Data Warehouse: AWS Redshift\
Orchestration: Apache Airflow

## Major Steps

-> Create data model: The data model forms the basis of your pipeline, informing the transformations that are required. Data model consists of various fact and dimension tables

-> Ingest data: Data is stored in AWS S3 as received. In this case the data is in a parquet file.

-> Transform data: Based on the data model convert the ingested file into multiple tables that can be loaded in a warehouse

-> Load data: Load the transformed data into AWS Redshift

-> Orchestrate the pipeline: This allows the pipeline to run whenever new data is availble or at fixed intervals e.g Once everyday, guaranteeing up-to-date metrics.

-> Build Dashboard: Create a view using SQL for the required data to build a dashboard. Use PowerBI to import data from the view and create appropriate Key Performance Indicators (KPIs) and visualizations.
