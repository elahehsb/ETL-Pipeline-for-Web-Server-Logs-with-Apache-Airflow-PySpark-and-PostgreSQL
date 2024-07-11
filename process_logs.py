from pyspark.sql import SparkSession
from pyspark.sql.functions import split, regexp_extract

# Initialize Spark session
spark = SparkSession.builder \
    .appName("WebServerLogProcessing") \
    .getOrCreate()

# Load the web server logs
log_df = spark.read.text("/opt/airflow/logs/web_server.log")

# Define the log pattern
log_pattern = r'(\S+) - - \[(\S+ -\d{4})\] "(\S+ \S+ \S+)" (\d{3}) (\d+)'

# Extract the fields from the log
logs_df = log_df.select(
    regexp_extract('value', log_pattern, 1).alias('ip_address'),
    regexp_extract('value', log_pattern, 2).alias('timestamp'),
    regexp_extract('value', log_pattern, 3).alias('request'),
    regexp_extract('value', log_pattern, 4).alias('status_code'),
    regexp_extract('value', log_pattern, 5).alias('response_size')
)

# Write the processed logs to a CSV file
logs_df.write.csv("/opt/airflow/scripts/processed_logs.csv", header=True)

# Stop the Spark session
spark.stop()
