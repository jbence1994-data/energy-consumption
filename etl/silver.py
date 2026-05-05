from os import environ as env

from pyspark.sql import Window
from pyspark.sql.functions import col, lead

from etl.helpers import get_spark_session, read_parquet, write_parquet_with_partitions


def silver_etl():
    spark = get_spark_session("energy_consumption_silver")

    silver_data_frame = read_parquet(spark, env.get("MEDALLION_BRONZE"))

    silver_data_frame = (silver_data_frame.withColumn(
        colName="daily_kwh",
        col=lead("kwh").over(Window.orderBy("year", "month", "day")) - col("kwh")
    ))

    silver_data_frame = silver_data_frame.dropna()

    write_parquet_with_partitions(silver_data_frame, env.get("MEDALLION_SILVER"), "year", "month")

    spark.stop()


if __name__ == "__main__":
    silver_etl()
