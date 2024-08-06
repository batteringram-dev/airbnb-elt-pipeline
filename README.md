# Airbnb Data Pipeline using Spark and Snowflake
This repository contains an Extract, Load, Transform (ELT) pipeline designed for Airbnb London listings data. The pipeline uses Spark for data processing and Snowflake as data warehouse. The dataset includes detailed information about each property, such as location, room type, pricing, and host details. 

## Architecture
1. **Extraction:** Extracts Airbnb data stored in Postgres
2. **Load:** Loads the data to Snowflake table
3. **Transform:** Transforms the data leveraging Spark on databricks notebook and pushed transformed data to Snowflake table (airbnb_listings_transformed)

## Tech Stack
1. Python: For scripting
2. Apache Spark: For data processing
3. Snowflake: For storing data (data warehousing)

## Workflow
1. Data is extracted from Postgres and loads the raw data it into Snowflake table (listings) using Apache Spark
2. Ingestion of data to Databricks and transformations are done here: \
   2.1: Data Quality - Cleaning null values and changing data types \
   2.2: Feature Engineering - Adding new columns based on price, reviews, and dates \
   2.3: Aggregations: Groupingby and window functions that solves a couple of problems \
3. Data Storage: Transformed data is pushed back to Snowflake to a different table so that we have both raw + transformed data

## Key Problems Solved By The Pipeline
1. What are the top 10 most expensive listing?
2. Average price for each type of room
3. Hosts who have the highest average listing price and manage the most listings
4. Check if the average price by host and room type correlates with the number of listings each host has
5. Average price in each neighbourhood
