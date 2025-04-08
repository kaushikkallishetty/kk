import re
from pyspark.sql.functions import from_csv


def extract_table_name(file):
  match = re.search(r"data/([^/]+)/", file)
  return match.group(1)


# this function generates the schema for from_cs
# convert - `"Id";"Name";"Created";`
# to - `Id string, Name string, Created string`
def generate_schema(header):
  schema = (
      ", "
      .join(map(lambda column: column.strip('"') + " string", header.split(";")))
  )

  return schema


def forEachBatch(df, batch_id):
  print(f"processing batch {batch_id}")
  
  first_row = df.head(1)[0]

  # determine the glue table name from the s3 file prefix
  table = extract_table_name(first_row["file_name"])
  table = table.lower()
  table = f"spark_catalog.nr_silver_billing_pfm_dl_dev.{table}"
  print(f"for table {table}, processing file {first_row['file_name']}")

  # determine the schema from the first record
  schema = generate_schema(first_row["value"])

  # parse the string records into a dataframe
  df = (
      df
      .select(from_csv("value", schema, {'sep': ';'}).alias("csv"))
      .select("csv.*")
      .offset(1)  # skip the header
  )

  # df.show(truncate=False)

  # create table if it doesn't exist
  if not df.sparkSession.catalog.tableExists(table):
    df.writeTo(table) \
      .tableProperty("write.spark.accept-any-schema", "true") \
      .using("iceberg") \
      .create()

  # append data to table
  # TODO: add de dupe logic
  df.writeTo(table) \
    .option("mergeSchema", "true") \
    .option("check-ordering", "false") \
    .append()


def write_df(df):
  return (
      df.writeStream
      .option("checkpointLocation", "s3a://dataos-billing-platform-api-ingestion-us-east-1-dev/spark-checkpoint/")
      .foreachBatch(forEachBatch)
      .start()
  )
