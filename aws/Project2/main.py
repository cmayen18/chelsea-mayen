# Importing the required PySpark module
from pyspark.sql import SparkSession

# Defining input and output paths on Amazon S3
S3_DATA_INPUT_PATH = "s3://s3-projectpro-emr-athena-sripad/source-folder/wikiticker-2015-09-12-sampled.json"
S3_DATA_OUTPUT_PATH_FILTERED = "s3://s3-projectpro-emr-athena-sripad/data-output/filtered"

# Main function to execute the PySpark processing
def main():
    # Creating a Spark session with the application name 'projectProDemo'
    spark = SparkSession.builder.appName('projectProDemo').getOrCreate()
    
    # Reading JSON data from the specified input path into a DataFrame
    df = spark.read.json(S3_DATA_INPUT_PATH)
    
    # Printing the total number of records in the source data set
    print(f'The total number of records in the source data set is {df.count()}')
    
    # Filtering the DataFrame based on conditions (not a robot and from the United States)
    filtered_df = df.filter((df.isRobot == False) & (df.countryName == 'United States'))
    
    # Printing the total number of records in the filtered data set
    print(f'The total number of records in the filtered data set is {filtered_df.count()}')
    
    # Displaying the first 10 rows of the filtered DataFrame
    filtered_df.show(10)
    
    # Printing the schema of the filtered DataFrame
    filtered_df.printSchema()
    
    # Writing the filtered data to the specified output path in Parquet format
    filtered_df.write.mode('overwrite').parquet(S3_DATA_OUTPUT_PATH_FILTERED)
    
    # Displaying a success message after the data has been written
    print('The filtered output is uploaded successfully')


if __name__ == '__main__':
    main()
