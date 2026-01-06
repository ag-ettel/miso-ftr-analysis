import os
import sys

# Set PYSPARK_PYTHON to the current Python executable
os.environ["PYSPARK_PYTHON"] = sys.executable

# Rest of your configuration
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth
from rtdip_sdk.pipelines.sources import MISOHistoricalLoadISOSource


# Critical Delta configuration
builder = SparkSession.builder \
    .appName("MISO Load Production") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")  # Critical for updates

spark = configure_spark_with_delta_pip(builder).getOrCreate()

miso_source = MISOHistoricalLoadISOSource(
    spark,
    options={
        "load_type": "actual",
        "start_date": "20250401",
        "end_date": "20260101",
        "region": "MISO North"
    }
)

df = miso_source.read_batch()
print(f"Successfully pulled {df.count()} records")

## partitioning
# 1. Extract date components (year/month/day)
# 2. Partition by year > month > day
# 3. Write as Delta Lake 
df = df.withColumn("year", year("Datetime")) \
       .withColumn("month", month("Datetime")) \
       .withColumn("day", dayofmonth("Datetime"))

# Write
(df.write
  .format("delta")  # Delta lake
  .partitionBy("year", "month", "day")  # date partitioning
  .mode("overwrite")
  .save("data/miso_load_actual"))

spark.stop()
