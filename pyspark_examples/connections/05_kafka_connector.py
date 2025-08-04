

# PySpark Cheatsheet: Kafka Connector (Structured Streaming)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct
from pyspark.sql.types import StructType, StringType, IntegerType

# Initialize Spark Session with Kafka package
# Ensure you have the correct package for your Spark version
spark = SparkSession.builder     .appName("pyspark-cheatsheet-kafka-connector")     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")     .getOrCreate()

# Kafka broker configuration
KAFKA_BOOTSTRAP_SERVERS = "your_kafka_bootstrap_servers:9092"  # Replace with your Kafka brokers
KAFKA_TOPIC_READ = "input_topic"   # Replace with your input topic
KAFKA_TOPIC_WRITE = "output_topic" # Replace with your output topic

# -----------------------------------------------------------------------------
# 1. Reading from a Kafka Topic (Streaming)
# -----------------------------------------------------------------------------

# Read from a Kafka topic as a streaming DataFrame
print(f"--- Reading from Kafka topic: {KAFKA_TOPIC_READ} ---")
df_kafka_stream = spark.readStream     .format("kafka")     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)     .option("subscribe", KAFKA_TOPIC_READ)     .option("startingOffsets", "latest")  # Can be "earliest" or "latest"
    .load()

# The Kafka source provides a DataFrame with the following schema:
# key, value, topic, partition, offset, timestamp, timestampType

# For this example, we assume the message value is a JSON string
# Define the schema for the JSON payload
schema = StructType([
    StringType("id"),
    StringType("product"),
    IntegerType("quantity")
])

# Deserialize the JSON value and select the fields
df_processed_stream = df_kafka_stream.selectExpr("CAST(value AS STRING)")     .select(from_json(col("value"), schema).alias("data"))     .select("data.*")

# -----------------------------------------------------------------------------
# 2. Writing to the Console (for debugging)
# -----------------------------------------------------------------------------

# Display the streaming DataFrame to the console
# This is useful for quick debugging and inspection
query_console = df_processed_stream.writeStream     .outputMode("append")     .format("console")     .start()

print("--- Streaming to console. Awaiting data... (run for a few moments) ---")
# In a real application, you would use query_console.awaitTermination()
# For this script, we'll let it run for a bit and then stop it.
# query_console.awaitTermination()

# -----------------------------------------------------------------------------
# 3. Writing to a Kafka Topic (Streaming)
# -----------------------------------------------------------------------------

# Before writing to Kafka, the DataFrame must have a `value` column.
# If you have multiple columns, you can serialize them to a JSON string.

print(f"--- Writing to Kafka topic: {KAFKA_TOPIC_WRITE} ---")
df_to_kafka = df_processed_stream.select(to_json(struct("*")).alias("value"))

# Write the streaming DataFrame to another Kafka topic
query_kafka = df_to_kafka.writeStream     .format("kafka")     .outputMode("append")     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)     .option("topic", KAFKA_TOPIC_WRITE)     .option("checkpointLocation", "/tmp/spark_kafka_checkpoint")  # Required for fault tolerance
    .start()

print("-- Streaming to Kafka. Awaiting data... --")

# In a real application, you would manage your queries
# For example, wait for all to terminate
# spark.streams.awaitAnyTermination()

# For this script, we'll stop them after a short period for demonstration
import time
time.sleep(30) # Let the streams run for 30 seconds

query_console.stop()
query_kafka.stop()

print("--- Streaming queries stopped. ---")
