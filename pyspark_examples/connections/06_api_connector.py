

# PySpark Cheatsheet: API Connector

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, StructType, StructField

# Initialize Spark Session
spark = SparkSession.builder     .appName("pyspark-cheatsheet-api-connector")     .getOrCreate()

# -----------------------------------------------------------------------------
# 1. Define a UDF to Fetch Data from an API
# -----------------------------------------------------------------------------

# This is a simple example using a public API that returns a random user.
API_URL = "https://randomuser.me/api/"

# Define the schema for the returned data
# This is important for creating a DataFrame from the API response
api_schema = StructType([
    StructField("gender", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True)
])

# Define a UDF that takes a URL, fetches the data, and returns it as a JSON string
@udf(StringType())
def fetch_api_data(url: str) -> str:
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes
        return response.text
    except requests.exceptions.RequestException as e:
        return f"Error: {e}"

# -----------------------------------------------------------------------------
# 2. Use the UDF to Create a DataFrame
# -----------------------------------------------------------------------------

# Create a DataFrame with a single column containing the API URL
# In a real-world scenario, you might have a list of URLs or IDs to process
df_urls = spark.createDataFrame([("1", API_URL)], ["id", "url"])

# Apply the UDF to the 'url' column to fetch the data
df_api_response = df_urls.withColumn("api_response", fetch_api_data(col("url")))

# At this point, the 'api_response' column contains the JSON string from the API
df_api_response.show(truncate=False)

# -----------------------------------------------------------------------------
# 3. Parse the JSON Response
# -----------------------------------------------------------------------------

from pyspark.sql.functions import from_json

# Define the schema to parse the nested JSON response from the API
response_schema = StructType([
    StructField("results", StringType(), True) # The API returns a 'results' array
])

# Parse the JSON string and extract the fields
df_parsed = df_api_response.withColumn("parsed_response", from_json(col("api_response"), response_schema))     .select("id", "parsed_response.results")

# The 'results' column is still a JSON string, so we need to parse it again
# This is specific to the structure of the randomuser.me API response
final_schema = StructType([
    StructField("gender", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True)
])

df_final = df_parsed.withColumn("user_data", from_json(col("results")[0], final_schema))     .select("id", "user_data.*")

df_final.show()

# -----------------------------------------------------------------------------
# Important Considerations
# -----------------------------------------------------------------------------

# - **Performance:** Using UDFs to call APIs can be slow, as it runs on a single executor per partition.
# - **Rate Limiting:** Be mindful of API rate limits. You may need to add delays or use a more sophisticated approach for large-scale API calls.
# - **Error Handling:** The UDF includes basic error handling, but you may need more robust error handling for production use cases.

