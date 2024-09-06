from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Orders(spark: SparkSession) -> DataFrame:
    if Config.fabricName == "dev":
        return spark.read\
            .schema(
              StructType([
                StructField("order_id", IntegerType(), True), StructField("customer_id", IntegerType(), True), StructField("order_status", StringType(), True), StructField("order_category", StringType(), True), StructField("order_date", StringType(), True), StructField("amount", DoubleType(), True)
            ])
            )\
            .option("header", True)\
            .option("sep", ",")\
            .option("ignoreLeadingWhiteSpace", True)\
            .option("ignoreTrailingWhiteSpace", True)\
            .csv("dbfs:/Prophecy/badbf9e6aa919d5b44424201c1c14c1c/OrdersDatasetInput.csv")
    else:
        raise Exception("No valid dataset present to read fabric")
