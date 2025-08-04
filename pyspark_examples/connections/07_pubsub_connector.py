

# PySpark Cheatsheet: Pub/Sub Connector (via GCS and Auto Loader)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType

# Initialize Spark Session
spark = SparkSession.builder     .appName("pyspark-cheatsheet-pubsub-connector")     .getOrCreate()

# -----------------------------------------------------------------------------
# 1. Configure Auto Loader to Read from a GCS Bucket
# -----------------------------------------------------------------------------

# This example assumes you have a Pub/Sub subscription that writes messages to a GCS bucket.
# Auto Loader will automatically detect new files in the bucket and process them.

# GCS path where Pub/Sub messages are stored
GCS_BUCKET_PATH = "gs://your-gcs-bucket/path/to/messages/"  # Replace with your GCS path

# Schema location for Auto Loader to track progress
SCHEMA_LOCATION = "/tmp/spark_pubsub_schema_location" # Use a persistent location in production

# Define the schema for the incoming JSON messages
message_schema = StructType([
    StringType("event_id"),
    StringType("event_type"),
    IntegerType("value")
])

# Read from the GCS bucket using Auto Loader
print(f"--- Reading from GCS bucket: {GCS_BUCKET_PATH} with Auto Loader ---")
df_pubsub_stream = spark.readStream     .format("cloudFiles")     .option("cloudFiles.format", "json")     .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)     .load(GCS_BUCKET_PATH)

# -----------------------------------------------------------------------------
# 2. Process the Streaming Data
# -----------------------------------------------------------------------------

# The `cloudFiles` format provides the raw data in a `value` column.
# We need to parse the JSON and apply the schema.

df_processed_stream = df_pubsub_stream.selectExpr("CAST(value AS STRING)")     .select(from_json(col("value"), message_schema).alias("data"))     .select("data.*")

# -----------------------------------------------------------------------------
# 3. Write the Output to the Console (for debugging)
# -----------------------------------------------------------------------------

# Display the processed data to the console
query = df_processed_stream.writeStream     .outputMode("append")     .format("console")     .start()

print("--- Streaming to console. Awaiting data... ---")

# In a real application, you would use query.awaitTermination()
# For this script, we'll let it run for a bit and then stop it.
# query.awaitTermination()

import time
time.sleep(30) # Let the stream run for 30 seconds

query.stop()

print("--- Streaming query stopped. ---")

# -----------------------------------------------------------------------------
# Important Considerations
# -----------------------------------------------------------------------------

# - **Permissions:** Ensure your Databricks cluster has the necessary permissions to read from the GCS bucket.
# - **Schema Evolution:** Auto Loader can handle schema evolution. You can configure it to merge schemas or fail on schema changes.
# - **Fault Tolerance:** Auto Loader uses checkpointing to track the files that have been processed, ensuring fault tolerance.

