# Databricks notebook source
! pip install -r requirements.txt

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   confederation,
# MAGIC   COUNT(DISTINCT country) AS total_countries,
# MAGIC   SUM(population_share) AS total_population_share,
# MAGIC   SUM(tv_audience_share) AS total_tv_audience_share,
# MAGIC   SUM(gdp_weighted_share) AS total_gdp_weighted_share
# MAGIC FROM
# MAGIC   fifa
# MAGIC GROUP BY
# MAGIC   confederation
# MAGIC ORDER BY
# MAGIC   total_gdp_weighted_share DESC;

# COMMAND ----------


