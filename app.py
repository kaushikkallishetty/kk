from _utils.spark_session import get_spark_session
from _utils.s3_utils import read_csv_from_s3
from _utils.iceberg_utils import write_df


def main():
  spark = get_spark_session()
  df = read_csv_from_s3(spark)
  query = write_df(df)
  query.awaitTermination()


if __name__ == "__main__":
  main()
