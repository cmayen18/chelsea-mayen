# Importing the required PySpark module
from pyspark.sql import SparkSession

# Defining input and output paths on Amazon S3
S3_DATA_INPUT_PATH = "s3://s3-projectpro-emr-athena/source-folder/wikiticker-2015-09-12-sampled.json"
S3_DATA_OUTPUT_PATH_AGGREGATED = "s3://s3-projectpro-emr-athena/data-output/aggregated"

# Main function to execute the PySpark processing
def main():
    # Creating a Spark session with the application name 'projectProDemo1'
    spark = SparkSession.builder.appName('projectProDemo1').getOrCreate()
    
    # Reading JSON data from the specified input path into a DataFrame
    df = spark.read.json(S3_DATA_INPUT_PATH)
    
    # Printing the total number of records in the input data set
    print(f'The total number of records in the input data set is {df.count()}')
    
    # Performing aggregation by grouping data based on the 'channel' column
    aggregated_df = df.groupBy(df.channel).count()
    
    # Printing the total number of records in the aggregated data set
    print(f'The total number of records in the aggregated data set is {aggregated_df.count()}')
    
    # Displaying the first 10 rows of the aggregated DataFrame
    aggregated_df.show(10)
    
    # Printing the schema of the aggregated DataFrame
    aggregated_df.printSchema()
    
    # Writing the aggregated data to the specified output path in Parquet format
    aggregated_df.write.mode('overwrite').parquet(S3_DATA_OUTPUT_PATH_AGGREGATED)
    
    # Displaying a success message after the data has been written
    print('The aggregated data has been uploaded successfully')


if __name__ == '__main__':
    main()
