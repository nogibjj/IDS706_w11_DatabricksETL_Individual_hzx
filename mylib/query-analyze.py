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


if __name__ == "__main__":
    query_transform()
    vis()