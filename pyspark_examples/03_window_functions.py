
# PySpark Cheatsheet: Window Functions

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number, rank, dense_rank, lag, lead, sum

# Initialize Spark Session
spark = SparkSession.builder.appName("pyspark-cheatsheet-window-functions").getOrCreate()

# Load the sample data
df_orders = spark.read.csv("/Users/rubenoreamunoarias/Documents/Github/sparkCheatSheet/data/sample_data.csv", header=True, inferSchema=True)

# -----------------------------------------------------------------------------
# 1. Window Specification
# -----------------------------------------------------------------------------

# A window specification defines the partitioning and ordering of the data.
# This window is partitioned by customer and ordered by the order date.
window_spec_by_customer = Window.partitionBy("customer_id").orderBy("order_date")

# -----------------------------------------------------------------------------
# 2. Ranking Functions
# -----------------------------------------------------------------------------

# row_number(): assigns a unique, sequential number to each row in the window
print("--- row_number() ---")
df_with_row_number = df_orders.withColumn("order_rank_in_customer", row_number().over(window_spec_by_customer))
df_with_row_number.show()

# rank(): assigns a rank to each row based on the ordering, with gaps for ties
print("--- rank() ---")
df_with_rank = df_orders.withColumn("order_rank", rank().over(window_spec_by_customer))
df_with_rank.show()

# dense_rank(): assigns a rank without gaps for ties
print("--- dense_rank() ---")
df_with_dense_rank = df_orders.withColumn("order_dense_rank", dense_rank().over(window_spec_by_customer))
df_with_dense_rank.show()

# -----------------------------------------------------------------------------
# 3. Analytic Functions
# -----------------------------------------------------------------------------

# lag(): accesses data from a previous row in the window
print("--- lag() ---")
df_with_lag = df_orders.withColumn("previous_order_date", lag("order_date", 1).over(window_spec_by_customer))
df_with_lag.show()

# lead(): accesses data from a subsequent row in the window
print("--- lead() ---")
df_with_lead = df_orders.withColumn("next_order_date", lead("order_date", 1).over(window_spec_by_customer))
df_with_lead.show()

# -----------------------------------------------------------------------------
# 4. Aggregate Functions over a Window
# -----------------------------------------------------------------------------

# Calculate the cumulative sum of the price for each customer
print("--- Cumulative Sum ---")
window_spec_cumulative = Window.partitionBy("customer_id").orderBy("order_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df_with_cumulative_sum = df_orders.withColumn("cumulative_price", sum("price").over(window_spec_cumulative))
df_with_cumulative_sum.show()
