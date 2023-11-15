"""
transform and load function
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

def load(dataset="dbfs:/FileStore/Week11_Databricks_ETL/fifa_countries_audience.csv"):
    spark = SparkSession.builder.appName("Read CSV").getOrCreate()
    # load csv and transform it by inferring schema 
    fifa_df = spark.read.csv(dataset, header=True, inferSchema=True)

    # add unique IDs to the DataFrames
    fifa_df = fifa_df.withColumn("id", monotonically_increasing_id())

    # transform into a delta lakes table and store it 
    fifa_df.write.format("delta").mode("overwrite").saveAsTable("fifa")
    num_rows = fifa_df.count()
    print(num_rows)
    
    return "finished transform and load"

if __name__ == "__main__":
    load()