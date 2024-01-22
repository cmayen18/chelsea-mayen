# Log File - Data Exploration

## Loading Libraries
from pyspark.sql import SparkSession

## Spark Session Object Creation
# To avoid org.apache.spark.SparkUpgradeException: A different result may be obtained due to the upgrading of Spark 3.0
# The Spark Session object is created with specified configurations for the time parser policy.

spark = SparkSession\
    .builder\
    .appName("pyspark-notebook")\
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
    .getOrCreate()

## Loading
# The log file path is assigned to "actual_log.txt"
# The base DataFrame is created by reading the text file.

log_file_path = "actual_log.txt"
base_df = spark.read.text(log_file_path)

# The schema of the DataFrame is displayed.
base_df.printSchema()

# Display the content of the DataFrame without truncation.
base_df.show(truncate=False)

## Parsing
# The individual columns are parsed using the special built-in regexp_extract() function.
# One regular expression is used for each field to be extracted.

# If regular expressions are found confusing, and exploration is desired, starting with the RegexOne web site is recommended.
from pyspark.sql.functions import split, regexp_extract

split_df = base_df.select(
    regexp_extract('value', r'^([^\s]+\s)', 1).alias('host'),
    regexp_extract('value', r'^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]', 1).alias('timestamp'),
    regexp_extract('value', r'^.*"\w+\s+([^\s]+)\s+HTTP.*"', 1).alias('path'),
    regexp_extract('value', r'^.*"\s+([^\s]+)', 1).cast('integer').alias('status'),
    regexp_extract('value', r'^.*\s+(\d+)$', 1).cast('integer').alias('content_size')
)

# Display the parsed DataFrame without truncation.
split_df.show(truncate=False)

## Cleaning
# The effectiveness of the parsing logic is examined.
# Verification of the absence of null rows in the original data set occurs first.

base_df.filter(base_df['value'].isNull()).count()

# The parsed DataFrame is checked for null values.
bad_rows_df = split_df.filter(
    split_df['host'].isNull() |
    split_df['timestamp'].isNull() |
    split_df['path'].isNull() |
    split_df['status'].isNull() |
    split_df['content_size'].isNull()
)

bad_rows_df.count()

# Identification of fields not populated in the original data is necessary.
from pyspark.sql.functions import col, sum

def count_null(col_name):
    return sum(col(col_name).isNull().cast('integer')).alias(col_name)

exprs = []
[exprs.append(count_null(col_name)) for col_name in split_df.columns]
split_df.agg(*exprs).show()

## Fix the rows with null content_size
# The null values in split_df are replaced with 0.
# The DataFrame API provides a set of functions and fields specifically designed for working with null values.
# The na is used to replace content_size with 0.

cleaned_df = split_df.na.fill({'content_size': 0})

exprs = []
[exprs.append(count_null(col_name)) for col_name in cleaned_df.columns]

cleaned_df.agg(*exprs).show()

from pyspark.sql.functions import *

logs_df = cleaned_df.select('*', to_timestamp(cleaned_df['timestamp'], "dd/MMM/yyyy:HH:mm:ss ZZZZ").cast('timestamp').alias('time')).drop('timestamp')

total_log_entries = logs_df.count()
print(total_log_entries)
logs_df.show(truncate=False)

# Display the DataFrame with a new 'time' column obtained by converting the 'timestamp' column to timestamp format.
cleaned_df.select('*').withColumn('time', to_timestamp(col("timestamp"), 'dd/MMM/yyyy:HH:mm:ss ZZZZ')) \
    .show(truncate=False)
