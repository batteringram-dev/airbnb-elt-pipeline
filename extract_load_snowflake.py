import json
from pyspark.sql import SparkSession


def main():
    '''
    Main function to execute the data pipeline that extracts Airbnb listings data from Postgres and dumps it into Snowflake.
    '''
    
    # Load config file
    with open("config.json", "r") as config_file:
        config = json.load(config_file)

    
    # Create Spark session
    spark = SparkSession \
        .builder \
        .appName("Postgres to Snowflake Load Session") \
        .config("spark.jars", "/home/batteringram-dev/Desktop/postgresql-42.7.2.jar,/home/batteringram-dev/Desktop/snowflake-jdbc-3.13.6.jar") \
        .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.9.1-spark_3.1") \
        .getOrCreate()


    # Print Spark session details for our reference
    print("Spark Version: ", spark.version)
    print("Spark Application Name: ", spark.sparkContext.appName)



    # Load data from PostgreSQL
    try:
        listings_df = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/AirbnbDB") \
            .option("dbtable", "listings") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .option("driver", "org.postgresql.Driver") \
            .load()

        # Show schema and data
        listings_df.printSchema()
        listings_df.show()
    except Exception as e:
        print(f"Error loading data from Postgres: ", e)

    # Snowflake options
    options = {
        "sfURL": config["sfURL"],
        "sfUser": config["sfUser"],
        "sfPassword": config["sfPassword"],
        "sfDatabase": config["sfDatabase"],
        "sfSchema": config["sfSchema"],
        "sfRole": config["sfRole"]
    }

    
    # Write data to Snowflake
    try:
        listings_df.write.format("snowflake") \
            .options(**options) \
            .option("dbtable", "listings.listings") \
            .mode("overwrite") \
            .save()

        print("Data loaded to Snowflake successfully.")
    except Exception as e:
        print(f"Error loading data to Snowflake: ", e)


if __name__ == "__main__":
    main()
