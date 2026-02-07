import os
import sys
import json
import time
import urllib.request
import urllib3
from datetime import datetime, timedelta
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    year, month, dayofmonth, lit, col, to_timestamp, abs as spark_abs
)
from pyspark.sql.types import (
    IntegerType, StructType, StructField, StringType, DoubleType, TimestampType
)
from pathlib import Path
# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent))
from utils import add_years, get_winter_dates, fetch_with_retry

## Set Spark environment
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["TZ"] = "UTC"
os.environ["SPARK_LOCALITY_WAIT"] = "30000"

## Delta configuration
builder = SparkSession.builder \
    .appName("MISO RT Fuel Mix") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .config("spark.sql.session.timeZone", "UTC")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

## MISO API configuration
API_BASE = "https://apim.misoenergy.org/lgi/v1"
SUBSCRIPTION_KEY = os.getenv('MISO_LOAD_KEY')
OUTPUT_DIR = "data/miso_rt_fuel_mix"

## Rate limiting configuration
REQUEST_DELAY = 0.8  # Seconds between requests
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2  # Exponential backoff base

## region of interest
TARGET_REGIONS = ["NORTH", "CENTRAL", "SOUTH", "EAST", "WEST"]

## main function to fetch RT fuel mix data
def fetch_rt_fuel_mix_for_date(target_date):
    """
    Fetch RT fuel mix data from MISO API for a specific date and target region
    :param target_date: specific date to fetch data for (datetime object)
    """
    date_str = target_date.strftime("%Y-%m-%d")
    all_records = []

    headers = {
        'Cache-Control': 'no-cache',
        'Ocp-Apim-Subscription-Key': SUBSCRIPTION_KEY
    }

    for region in TARGET_REGIONS:
        print(f" Fetching RT fuel mix for {date_str} in region {region}...")
        
        page = 1
        max_pages = 10
        while page <= max_pages:
            url = f"{API_BASE}/real-time/{date_str}/generation/fuel-type?pageNumber={page}&region={region}"

            json_data = fetch_with_retry(url, headers)

            if json_data is None:
                # Either 404 or failed after retries
                break

            # Extract the list from the "data" key
            data = json_data.get("data", [])

            if not data or len(data) == 0:
                break

            # Flatten each record
            for item in data:
                # Extract time interval components
                time_interval = item.get("timeInterval", {})
                fuel_types = item.get("fuelTypes", {})

                record = {
                    'region': item.get('region'),
                    'start_time': time_interval.get('start'),
                    'end_time': time_interval.get('end'),
                    'resolution': time_interval.get('resolution'),
                    'interval_value': time_interval.get('value'),
                    'totalMw': item.get('totalMw'),
                    'coal': fuel_types.get('coal'),
                    'gas': fuel_types.get('gas'),
                    'nuclear': fuel_types.get('nuclear'),
                    'water': fuel_types.get('water'),
                    'wind': fuel_types.get('wind'),
                    'solar': fuel_types.get('solar'),
                    'other': fuel_types.get('other'),
                    'storage': fuel_types.get('storage'),
                    'demand_type': 'rt_fuel_mix'
                }
                all_records.append(record)

            # Check if we've reached the last page for this node
            if len(data) < 1000:
                break

            page += 1

    return all_records

def process_date_range(start_date, end_date):
    """
    Process RT fuel mix for date range and save to Delta table
    :param start_date: start date (datetime object)
    :param end_date: end date (datetime object)
    """
    schema = StructType([
        StructField("region", StringType(), True),
        StructField("start_time", StringType(), True),  # will convert to Timestamp later
        StructField("end_time", StringType(), True),    # will convert to Timestamp later
        StructField("resolution", StringType(), True),
        StructField("interval_value", StringType(), True),
        StructField("totalMw", DoubleType(), True),
        StructField("coal", DoubleType(), True),
        StructField("gas", DoubleType(), True),
        StructField("nuclear", DoubleType(), True),
        StructField("water", DoubleType(), True),
        StructField("wind", DoubleType(), True),
        StructField("solar", DoubleType(), True),
        StructField("other", DoubleType(), True),
        StructField("storage", DoubleType(), True),
        StructField("demand_type", StringType(), True),
        StructField("year", StringType(), True),
        StructField("month", StringType(), True),
        StructField("day", StringType(), True)
    ])

    all_records = []
    current_date = start_date

    while current_date <= end_date:
        print(f"Processing date: {current_date.strftime('%Y-%m-%d')}")
        date_records = fetch_rt_fuel_mix_for_date(current_date)

        if date_records:
            all_records.extend(date_records)
        else:
            print(f"  No records fetched for {current_date.strftime('%Y-%m-%d')}")

        current_date += timedelta(days=1)

    if all_records:
        print(f"\nCreating DataFrame with {len(all_records)} total records...")

        # Create single DataFrame (one staging operation instead of per-date)
        temp_df = spark.createDataFrame(all_records, schema=schema)

        # Convert timestamps and add partitioning columns
        processed_df = temp_df \
            .withColumn("start_time", to_timestamp(col("start_time"), "yyyy-MM-dd'T'HH:mm:ss")) \
            .withColumn("end_time", to_timestamp(col("end_time"), "yyyy-MM-dd'T'HH:mm:ss")) \
            .withColumn("year", year(col("start_time"))) \
            .withColumn("month", month(col("start_time"))) \
            .withColumn("day", dayofmonth(col("start_time")))

        record_count = processed_df.count()
        print(f"  Total records after processing: {record_count}")

        # Data quality check
        quality_df = processed_df.filter(
            (col("totalMw").isNotNull()) & (col("totalMw") >= 0)
        )

        quality_count = quality_df.count()
        print(f"  Records after data quality filtering: {quality_count} (removed {record_count - quality_count})")
        record_count = quality_count

        # partition to Delta table using merge (preserves existing data)
        if DeltaTable.isDeltaTable(spark, OUTPUT_DIR):
            # Table exists - merge new data
            target_table = DeltaTable.forPath(spark, OUTPUT_DIR)

            target_table.alias("target") \
                .merge(
                    quality_df.alias("source"),
                    "target.region = source.region AND target.start_time = source.start_time"
                ) \
                .whenNotMatchedInsertAll() \
                .execute()

            print(f"Merged {record_count} records into existing Delta table")
        else:
            # Table doesn't exist - create it
            (quality_df.write
                .format("delta")
                .partitionBy("year", "month", "day")
                .mode("overwrite")
                .save(OUTPUT_DIR))

            print(f"Created new Delta table with {record_count} records")

            return record_count  
    else:
        print("No records to write.")
        return 0

## Main execution
try:
    today = datetime.utcnow().date()
    start_date = today - timedelta(days=5) # add_years(today, -3)  # 3 years ago
    end_date = today - timedelta(days=1)  # up to yesterday to ensure complete data

    print(f"Starting RT fuel mix data processing from {start_date} to {end_date}...")
    record_count = process_date_range(start_date, end_date)
    
# Each winter season runs December-February
#    winter_years = [2014, 2015] # 2018, 2019, 2020, 2021,   # 2020-2021 captures Uri, 2022-2023 captures Elliott

#    for dec_year in winter_years:
#        start_date, end_date = get_winter_dates(dec_year)
#        print(f"\n=== Processing winter {dec_year}-{dec_year+1} from {start_date} to {end_date} ===")
    
#        record_count = process_date_range(start_date, end_date)
#        print(f"Completed winter {dec_year}-{dec_year+1}, fetched {record_count} records")

#    record_count = process_date_range(start_date, end_date)

except Exception as e:
    print(f"Error during processing: {str(e)}")
    import traceback
    traceback.print_exc()

finally:
    spark.stop()
    print("Spark session stopped.")
