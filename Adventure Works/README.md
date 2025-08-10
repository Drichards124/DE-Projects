<div align="center">

# AdventureWorks Data Engineering Pipeline

</div>

## Overview

This project showcases a complete data engineering workflow built on Azure, following the medallion architecture (bronze, silver, gold) to ensure clean, scalable, and well-structured data processing. The pipeline integrates GitHub for data sourcing, Azure Data Factory for ingestion, Azure Databricks for transformation, Azure Synapse Analytics for serving, and Power BI for reporting.

### Data Architecture 

<img width="1143" height="583" alt="image" src="https://github.com/user-attachments/assets/15c03262-f154-4dee-9b47-d1877cc1c896" />

##  Data Ingestion to Bronze Layer

The pipeline begins with CSV files stored in the /data directory of this GitHub repository. Azure Data Factory (ADF) retrieves these files via an HTTP Linked Service and deposits them into the bronze layer of Azure Data Lake Storage Gen2.

This ingestion process is fully dynamic:
	
 •	Parameter-driven — relative file paths, folder destinations, and file names are controlled by parameters.
 
 •	JSON-based configuration — ingestion logic is stored in the JSON file in /logic directory, allowing new datasets to be added without code changes.
 
 •	Scalable — designed to handle multiple datasets in a single pipeline run through iteration.

By the end of this stage, the bronze layer contains an organized, up-to-date collection of raw CSV files from GitHub, ready for transformation in the next phase.

## Data Transformation (Silver Layer)

The transformation phase is powered by Azure Databricks using PySpark.
	
 •	Secure Connectivity — Databricks was configured to access Azure Data Lake using an Azure App Registration.
 
 •	Microsoft Documentation was followed to set up the connection, leveraging managed identity and client secret credentials for secure authentication.
 
 •	Minor Transformations — Data cleaning and light preprocessing were applied, preparing the datasets for analysis.
 
 •	Optimized Storage — The transformed datasets were written back to the silver layer of Azure Data Lake as Parquet files, improving query performance and downstream processing.

 ## Data Serving (Gold Layer)

The curated silver-layer data is served to analytics consumers through Azure Synapse Analytics (serverless SQL).
	
 •	Schema Creation: Defined a schema to organize gold-layer objects.
 
 •	Data Access: Queried silver-layer data using the OPENROWSET function then created views for simplified querying.
 
 •	Cost Efficiency: All processing stored in a serverless database to optimize costs.
 
 •	Gold Storage Push:
 
           1.	Created a master encryption key.
          	
           2.	Created a database scoped credential (using managed identity).
          	
           3.	Defined an external data source pointing to the gold container.
          	
           4.	Configured an external file format (Parquet with Snappy compression).
          	
           5.	Created external tables mapping to gold-layer parquet files.

The gold container in Azure Data Lake now holds optimized, analytics-ready datasets for reporting.

## Reporting (Power BI)

The gold-layer data was connected to Power BI to deliver insights through an interactive Sales Trend Dashboard.
	
 •	Purpose: Provide stakeholders with a clear view of sales performance and answer key business questions.
	
 •	Key Visuals: Monthly sales trends, year-over-year comparisons, category-level breakdowns.
	
 •	Output: A PDF export of the dashboard is available in the /output directory for reference.
