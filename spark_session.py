from pyspark.sql import SparkSession
from pyspark import SparkConf
import os


def get_spark_session():
  conf = (
      SparkConf()
      .set("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
      .set("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
      .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
      # .set("spark.sql.catalog.spark_catalog.type", "glue")
      .set("spark.sql.catalog.spark_catalog.glue.catalog-endpoint", f"glue.{os.getenv('AWS_REGION')}.amazonaws.com")
      .set("spark.sql.catalog.spark_catalog.glue.region", os.getenv("AWS_REGION"))
      .set("spark.sql.catalog.spark_catalog.warehouse", "s3a://nr-silver-billing-pfm-dl-us-east-1-dev")
  )

  spark = SparkSession.builder.config(conf=conf).getOrCreate()
  return spark
