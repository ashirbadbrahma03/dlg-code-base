# importing the libraries
import boto3
import logging
import sys
import traceback
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'file_path', 'output_path'])

# Defining glue and spark sessions
sc = SparkContext()
sqlcontext = SQLContext(sc)
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

# Defining boto3 libraries for different services
s3_resource = boto3.resource('s3')

# Defining objects for the log services
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
handler.setLevel(logging.INFO)
log.addHandler(handler)

# Reading different parameters

file_path = args["file_path"]
output_path = args["output_path"]


def main():
    """ This function is the main function of teh program that calls the other function to process the input files in S3."""
    try:
        # File path hardcoded for now.Will be automated as part of event driven process.
        # file_path = 's3://dlginput/input-data/'

        # steps to construct spark dataframe out of the input files
        dlginputdf = sqlcontext.read.csv(file_path, header='true')
        dlginputconverteddf = dlginputdf.withColumn("ScreenTemperature", dlginputdf.ScreenTemperature.cast('float'))

        # Creat temp view on top of the spark dataframe created out of the input files.
        dlginputconverteddf.createOrReplaceTempView("dlginput")

        row_count = spark.sql("select count(*) as record_count from dlginput")

        log.info("Record count for data processing %s" % (row_count.show()))

        # Creating a new data frame with selected columns and filtering the assumed unknown rows i.e. screentemp = -99

        final_df = spark.sql(
            "select ObservationDate,Country,Region,SiteName,max(ScreenTemperature) as Max_Day_Temp from dlginput where ScreenTemperature != -99 Group by ObservationDate,Country,Region,SiteName")

        # log.info("Today's final df count %s", final_df.show())

        # Creat temp view on top of the spark dataframe created out of the final outputdf.
        final_df.createOrReplaceTempView("finaloutputdf")

        # Generating requested stats from the question - Optionla. The final df is the end point which will be used for the queries.
        hottest_temp = spark.sql(
            "select finaloutputdf.ObservationDate as Hottest_Date,finaloutputdf.Country as Hottest_Country,finaloutputdf.Region as Hottest_Region,finaloutputdf.Max_Day_Temp as Max_Temp from finaloutputdf join (select max(Max_Day_Temp) as hottest_temp from finaloutputdf) as test on  finaloutputdf.Max_Day_Temp = test.hottest_temp")

        # Write the output to the dataframe
        # reponse_par = final_df.write.mode("overwrite").option("header","true").parquet(f"s3://dlginput/output-data/")
        reponse_par = final_df.write.mode("overwrite").option("header", "true").parquet(f"{output_path}")
        log.info("Successfully processed the input file to the output location")

    except Exception as e:
        log.error("Error while processing the input file %s" % (str(e)))
        log.error(logging.traceback.format_exc())
        raise


if __name__ == "__main__":
    """Used to call the main function"""
    main()


