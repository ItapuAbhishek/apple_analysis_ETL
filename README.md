# apple_analysis_ETL

PySpark ETL Framework for Apple Analysis
This project implements a modular ETL framework using PySpark to process and analyze data related to customer transactions. It uses well-structured design patterns like Factory Pattern and Abstract Base Classes to ensure the framework is extensible, maintainable, and optimized for real-world applications.

Project Overview
The project contains two main ETL workflows that analyze customer transactions:

First Workflow: Identifies customers who bought AirPods immediately after purchasing an iPhone.
Second Workflow: Identifies customers who bought only iPhones and AirPods, with no other products.
Key Features
Extractor-Transformer-Loader (ETL): Modular and well-defined stages for data processing.
Factory Design Pattern:
Dynamically selects data readers (Reader Factory) and data sinks (Loader Factory).
Modern Data Formats: Handles CSV, Parquet, and Delta Lake.
Spark Optimization Techniques:
Broadcast joins
Window functions (e.g., LEAD and LAG)
Partitioning and bucketing for efficient data storage.
Highly Extensible: Supports adding new data sources, sinks, and transformations with minimal changes.
Project Structure
1. Reader Factory
Manages data extraction by supporting multiple data formats.

CSVDataSource: Reads data from CSV files.
ParquetDataSource: Reads data from Parquet files.
DeltaDataSource: Reads data from Delta tables.
Factory Method: get_data_source(data_type, file_path) dynamically selects the appropriate reader.
2. Extractor
Abstracts the data extraction process for different workflows.

AirpodsAfteriPhoneExtractor:
Reads transaction data from CSV.
Reads customer data from Delta Lake.
Outputs the extracted data as a dictionary of DataFrames.
3. Transformer
Implements business logic for transforming the data.

AirpodsAfterIphoneTransformer:
Identifies customers who bought AirPods immediately after buying iPhones using Spark SQL window functions.
OnlyAirpodsAndIphoneTransformer:
Identifies customers who bought only iPhones and AirPods using aggregation and filtering.
4. Loader Factory
Manages data loading by supporting multiple sinks.

LoadToDBFS: Writes data to DBFS without partitioning.
LoadToDBFSWithPartition: Writes data to DBFS with partitioning.
LoadToDeltaTable: Writes data to Delta tables.
Factory Method: get_sink_source(sink_type, df, path, method, params) dynamically selects the appropriate loader.
5. Workflows
Manages the execution of specific ETL pipelines.

FirstWorkFlow: Runs the ETL pipeline for identifying customers who bought AirPods after iPhones.
SecondWorkFlow: Runs the ETL pipeline for identifying customers who bought only iPhones and AirPods.
WorkFlowRunner: Dynamically triggers the desired workflow based on input.
How It Works
Workflow Execution
Extraction: Reads raw data from various sources using the Reader Factory.
Transformation: Applies business logic to extract meaningful insights.
Loading: Writes the transformed data to storage (DBFS or Delta Lake).
