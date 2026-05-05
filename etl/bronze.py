from os import environ as env

from etl.helpers import get_spark_session, read_csv, write_parquet


def bronze_etl():
    spark = get_spark_session("energy_consumption_bronze")

    bronze_data_frame = read_csv(spark, env.get("DATA"))

    write_parquet(bronze_data_frame, env.get("MEDALLION_BRONZE"))

    spark.stop()


if __name__ == "__main__":
    bronze_etl()
