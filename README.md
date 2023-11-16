[![CI](https://github.com/nogibjj/python-ruff-template/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/python-ruff-template/actions/workflows/cicd.yml)
# Individual Project 3 - Databricks ETL pipeline

# Requirements
Your project should include the following:
- A well-documented Databricks notebook that performs ETL (Extract, Transform, Load) operations, checked into the repository.
- Usage of Delta Lake for data storage (show you understand the benefits).
- Usage of Spark SQL for data transformations.
- Proper error handling and data validation.
- Visualization of the transformed data.
- An automated trigger to initiate the pipeline.
- README.md: A file that clearly explains what the project does, its dependencies, how to run the program, and concludes with actionable and data-driven recommendations to a hypothetical management team.
- Video Demo: A YouTube link in README.md showing a clear, concise walkthrough and demonstration of your ETL pipeline, including the automated trigger and recommendations to the management team.

# Preparation
1. Create the Azure databricks workspace and cluster
2. Connect the Github account to the databricks workspace(User settings -> Linked accounts)
3. Set up the environment variables using `Global ini scripts` or `.env` file
    - Configuration for the `SERVER_HOSTNAME` and `ACCESS_TOKEN`
4. Clone the Github repo into Databricks worspace
5. Create a job on Databricks to build an ETL pipeline
6. Set the auto trigger

Key files:
- mylib
    * `extract.py`
    * `transform_load.py`
    * `query_visual.py`
* `Databricks_ETL.ipynb`
* `.env`(hidden)

# Data Source
The data file `fifa_countries_audience.csv` includes the following variables:

Header | Definition
---|---------
`country` | FIFA member country
`confederation` | Confederation to which country belongs
`population_share` | Country's share of global population (percentage)
`tv_audience_share` | Country's share of global world cup TV Audience (percentage)
`gdp_weighted_share` | Country's GDP-weighted audience share (percentage)



# References
1. https://docs.databricks.com/en/dbfs/filestore.html



