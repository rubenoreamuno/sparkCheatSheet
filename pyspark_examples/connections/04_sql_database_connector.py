

# PySpark Cheatsheet: SQL Database Connector

from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder     .appName("pyspark-cheatsheet-sql-connector")     .getOrCreate()

# -----------------------------------------------------------------------------
# 1. Reading from a SQL Database
# -----------------------------------------------------------------------------

# Database connection properties
# Replace with your actual database connection details
jdbc_url = "jdbc:postgresql://your_database_host:5432/your_database_name"
connection_properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "org.postgresql.Driver"  # Example for PostgreSQL
}

# Read data from a table
# The `dbtable` option can be a table name or a subquery
print("--- Reading from a SQL database ---")
df_from_db = spark.read.jdbc(
    url=jdbc_url,
    table="your_table_name",
    properties=connection_properties
)

df_from_db.show(5)

# Example with a subquery
# This is useful for more complex queries
query = "(SELECT * FROM your_table_name WHERE your_condition) AS subquery"
df_from_query = spark.read.jdbc(
    url=jdbc_url,
    table=query,
    properties=connection_properties
)

df_from_query.show(5)

# -----------------------------------------------------------------------------
# 2. Writing to a SQL Database
# -----------------------------------------------------------------------------

# Create a sample DataFrame to write
data = [("John", 30), ("Jane", 25)]
columns = ["name", "age"]
df_to_write = spark.createDataFrame(data, columns)

# Write data to a table
# The `mode` option can be "append", "overwrite", "ignore", or "error"
print("-- Writing to a SQL database ---")
df_to_write.write.jdbc(
    url=jdbc_url,
    table="new_table_name",
    mode="overwrite",  # Be careful with overwrite in production
    properties=connection_properties
)

print("Data successfully written to the database.")

