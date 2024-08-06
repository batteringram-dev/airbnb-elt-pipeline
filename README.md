# Airbnb Data Pipeline using Spark and Snowflake
This repository contains an Extract, Load, Transform (ELT) pipeline designed for Airbnb London listings data. The pipeline using Spark for data processing and Snowflake as data warehouse. 

## Key Features
1. Extraction: Extracts Airbnb data stored in Postgres
2. Load: Loads the data to Snowflake table (listings)
3. Transform: Transforms the data leveraging Spark on databricks notebook and pushed transformed data to Snowflake table (airbnb_listings_transformed)

## Tech Stack
1. Python: For scription
2. Apache Spark: For data processing
3. Snowflake: For storing data (data warehousing)
