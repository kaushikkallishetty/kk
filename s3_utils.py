from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name

# ensure only one file is read at a time
# this ensures that each microbatch
# has data for only one table at a time
# additionally, archive the files that have already been read
def read_csv_from_s3(spark: SparkSession):
  return (
      spark.readStream
      .format("text")
      .option("maxFilesPerTrigger", "1")
      .option("cleanSource", "archive")
      .option("sourceArchiveDir", "s3a://dataos-billing-platform-api-ingestion-us-east-1-dev/archive")
      .load("s3a://dataos-billing-platform-api-ingestion-us-east-1-dev/data/*/*")
      .withColumn("file_name", input_file_name())
  )
