

# PySpark Cheatsheet: SFTP Connector

import pysftp
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Initialize Spark Session
spark = SparkSession.builder     .appName("pyspark-cheatsheet-sftp-connector")     .getOrCreate()

# -----------------------------------------------------------------------------
# 1. Define a UDF to Read a File from an SFTP Server
# -----------------------------------------------------------------------------

# SFTP connection details
# Replace with your actual SFTP server details
SFTP_HOSTNAME = "your_sftp_hostname"
SFTP_USERNAME = "your_sftp_username"
SFTP_PASSWORD = "your_sftp_password"

@udf(StringType())
def read_sftp_file(remote_path: str) -> str:
    try:
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None  # Disable host key checking (not recommended for production)
        with pysftp.Connection(SFTP_HOSTNAME, username=SFTP_USERNAME, password=SFTP_PASSWORD, cnopts=cnopts) as sftp:
            with sftp.open(remote_path) as f:
                return f.read().decode("utf-8")
    except Exception as e:
        return f"Error: {e}"

# -----------------------------------------------------------------------------
# 2. Use the UDF to Read a File and Create a DataFrame
# -----------------------------------------------------------------------------

# Create a DataFrame with the remote file path
# In a real-world scenario, you might have a list of files to process
df_filepath = spark.createDataFrame([("/remote/path/to/your/file.csv",)], ["remote_path"])

# Apply the UDF to read the file content
df_file_content = df_filepath.withColumn("file_content", read_sftp_file("remote_path"))

# At this point, the 'file_content' column contains the content of the file
df_file_content.show(truncate=False)

# -----------------------------------------------------------------------------
# 3. Parse the File Content
# -----------------------------------------------------------------------------

# The file content is now in a single string. You can parse it using Spark's
# built-in functions. For example, if the file is a CSV, you can use `from_csv`.

from pyspark.sql.functions import from_csv
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define the schema for the CSV data
csv_schema = StructType([
    StructField("col1", StringType(), True),
    StructField("col2", IntegerType(), True)
])

# Parse the CSV string
df_parsed = df_file_content.withColumn("parsed_data", from_csv("file_content", csv_schema))     .select("parsed_data.*")

df_parsed.show()

# -----------------------------------------------------------------------------
# Important Considerations
# -----------------------------------------------------------------------------

# - **Security:** Storing credentials in the code is not recommended for production. Use a secrets manager like Databricks Secrets or HashiCorp Vault.
# - **Performance:** Using a UDF to read files from SFTP can be slow. For large files, it's better to download the files to a distributed file system (like DBFS or GCS) first, and then process them with Spark.
# - **Error Handling:** The UDF includes basic error handling, but you may need more robust error handling for production use cases.

