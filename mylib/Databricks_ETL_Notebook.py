# Databricks notebook source
! pip install -r ../requirements.txt

# COMMAND ----------

import requests
from dotenv import load_dotenv
import os
import json
import base64

# Load environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/FileStore/Week11_Databricks_ETL"
headers = {'Authorization': 'Bearer %s' % access_token}
url = "https://"+server_h+"/api/2.0"


def perform_query(path, headers, data={}):
    session = requests.Session()
    resp = session.request('POST', url + path, 
                           data=json.dumps(data), 
                           verify=True, 
                           headers=headers)
    return resp.json()


def mkdirs(path, headers):
    _data = {}
    _data['path'] = path
    return perform_query('/dbfs/mkdirs', headers=headers, data=_data)
  

def create(path, overwrite, headers):
    _data = {}
    _data['path'] = path
    _data['overwrite'] = overwrite
    return perform_query('/dbfs/create', headers=headers, data=_data)


def add_block(handle, data, headers):
    _data = {}
    _data['handle'] = handle
    _data['data'] = data
    return perform_query('/dbfs/add-block', headers=headers, data=_data)


def close(handle, headers):
    _data = {}
    _data['handle'] = handle
    return perform_query('/dbfs/close', headers=headers, data=_data)


def uploadFile(url, dbfs_path, overwrite, headers):
    response = requests.get(url)
    if response.status_code == 200:
        content = response.content
        handle = create(dbfs_path, overwrite, headers=headers)['handle']
        print("Putting file: " + dbfs_path)
        for i in range(0, len(content), 2**20):
            add_block(handle, 
                      base64.standard_b64encode(content[i:i+2**20]).decode(), 
                      headers=headers)
        close(handle, headers=headers)
        print(f"File {dbfs_path} uploaded successfully.")
    else:
        print(f"Error downloading file from {url}. Status code: {response.status_code}")


def extract(
        url="""https://github.com/fivethirtyeight/data/blob/master/fifa/fifa_countries_audience.csv?raw=true""",
        file_path=FILESTORE_PATH+"/fifa_countries_audience.csv",
        directory=FILESTORE_PATH,
        overwrite=True
):
    """Extract a url to a file path"""
    # Make the directory, no need to check if it exists or not
    mkdirs(path=directory, headers=headers)
    # Add the csv files, no need to check if it exists or not
    uploadFile(url, file_path, overwrite, headers=headers)

    return file_path


# COMMAND ----------

extract()

# COMMAND ----------

"""
transform and load function
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

def load(dataset="dbfs:/FileStore/Week11_Databricks_ETL/fifa_countries_audience.csv"):
    spark = SparkSession.builder.appName("Read CSV").getOrCreate()
    # load csv and transform it by inferring schema 
    fifa_df = spark.read.csv(dataset, header=True, inferSchema=True)
    # cleaning and preparing the data for analysis
    fifa_df = fifa_df.dropna()
    # add unique IDs to the DataFrames
    fifa_df = fifa_df.withColumn("id", monotonically_increasing_id())

    # transform into a delta lakes table and store it 
    fifa_df.write.format("delta").mode("overwrite").saveAsTable("fifa")
    num_rows = fifa_df.count()
    print(num_rows)
    
    return "finished transform and load"

# COMMAND ----------

load()

# COMMAND ----------

from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import pandas as pd


# sample query
def query_transform():
    """
    Run a predefined SQL query on a Spark DataFrame.

    Returns:
        DataFrame: Result of the SQL query.
    """
    spark = SparkSession.builder.appName("Query").getOrCreate()
    query = (
        """
        SELECT
            confederation,
            COUNT(DISTINCT country) AS total_countries,
            SUM(population_share) AS total_population_share,
            SUM(tv_audience_share) AS total_tv_audience_share,
            SUM(gdp_weighted_share) AS total_gdp_weighted_share
        FROM
            fifa
        GROUP BY
            confederation
        ORDER BY
            total_gdp_weighted_share DESC
        """
    )
    query_result = spark.sql(query)
    return query_result


# sample viz for project
def vis():
    query = query_transform()
    count = query.count()
    if count > 0:
        print(f"Data validation passed. {count} rows available.")
    else:
        print("No data available. Please investigate.")

    # Transform Spark DataFrame into pandas DataFrame
    data = query.toPandas()

    # Create a new DataFrame that retains only the weighted share of total GDP and the Federation
    gdp_weighted_share = data.loc[:,['confederation', 'total_gdp_weighted_share']]
    gdp_weighted_share = gdp_weighted_share.sort_values(by='total_gdp_weighted_share', ascending=False)

    # Bar plot
    fig, ax = plt.subplots(figsize=(8, 6))

    ax.bar(gdp_weighted_share.confederation, 
        gdp_weighted_share.total_gdp_weighted_share, 
        color='blue')

    ax.set_title('Total GDP grouped by confederation', fontsize=14)
    ax.set_xlabel('confederation', fontsize=12)
    ax.set_ylabel('Total GDP(%)', fontsize=12)
    plt.xticks(rotation=60)
    plt.show()

# COMMAND ----------

query_transform()

# COMMAND ----------

vis()

# COMMAND ----------


