# Windmill data analysis

This repo contains two implementations of pipeline that forms simple medallion architecture from data provided as CSV-files. Files are read from Azure data lake storage which is mapped as volume to Unity Catalog.

- **Bronze layer** ingests data from CSV-files and materializes it to Unity Catalog tables in a raw format
- **Silver layer** cleans the data by removing rows that contain empty values and casts the data to reasonable data types. 
- **Gold layer** contains summarized daily averages per turbine and the rows where power output is over 2 standard deviotions from daily average

### Implementation Delta Live Tables
The dlt-notebook is in ./dlt -directory. It contains bronze layer as materialized view, silver layer as view and gold layer items as materialized views for querying.

DLT implementation contains basic data validations as dlt-expectations

DLT implementation can be run by creating new dlt-pipeline with source code in dlt/windmills -notebook. Path to source files should be added to SOURCE_PATH -variable value in first cell of the notebook 

### Implementation with  DBT core 
DBT implementation is in ./dbt -directory. It contains dbt-project.yml -file and models-directory containing the dbt-models as sql scripts. DBT creates bronze and silver layer as tables and gold layer items as views.

DBT implementation can be run e.g. in Databricks Workflows by defining dlt-job and referring to the dbt-directory. Additionally profiles.yml should be added to the dbt-directory with profile named wind_dbt. Path to source files should be added to SQL script in models/widmill_bronze.sql replacing <path to files>

