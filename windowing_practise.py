from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, lead
from pyspark.sql.window import Window


def products():
    spark: SparkSession = (SparkSession.builder
                           .master("local[*]")
                           .appName("practise")
                           .getOrCreate())

    products_data_frame = (spark
                           .read
                           .option(key="header", value="true")
                           .option(key="inferSchema", value="true")
                           .csv("D:/!projects/energy-consumption/products.csv"))

    window = (Window
              .partitionBy("category")
              .orderBy(col("price")))

    products_data_frame = (products_data_frame
                           .withColumn("next price", lead("price").over(window)))

    products_data_frame.show(truncate=False, n=products_data_frame.count())


if __name__ == "__main__":
    products()
