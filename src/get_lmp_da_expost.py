import os
import sys
import json
import time
import urllib.request
import urllib3
from datetime import datetime, timedelta
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    year, month, dayofmonth, lit, col, to_timestamp, abs as spark_abs
)
from pyspark.sql.types import (
    IntegerType, StructType, StructField, StringType, DoubleType, TimestampType
)

## Set Spark environment
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["TZ"] = "UTC"
os.environ["SPARK_LOCALITY_WAIT"] = "30000"

## Delta configuration
builder = SparkSession.builder \
    .appName("MISO LMP RT Expost Production") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .config("spark.sql.session.timeZone", "UTC")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

## MISO API configuration
API_BASE = "https://apim.misoenergy.org/pricing/v1"
SUBSCRIPTION_KEY = os.getenv('MISO_PRICING_KEY')
OUTPUT_DIR = "data/miso_lmp_da_expost"

## Rate limiting configuration
REQUEST_DELAY = 0.5  # Seconds between requests
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2  # Exponential backoff base

## GRE nodes of interest
TARGET_NODES = [
    "GRE.CAMBRCAMB2",  # Cambridge peaker
    "GRE.CAMBRCAMB",   # Cambridge unit 1 (not peaker, but for comparison)
    "GRE.ELKRI4",      # Elk River peaker
    "OTP.GRE.DEUEL",   # MN/SD border node
    "GRE.NSP.GREC",    # GRE aggregated load zone node
    "MINN.HUB"         # Minnesota Hub
]

def add_years(d, years):
    """Return a date that's `years` years after the date (or before if negative).
    Handles leap year edge cases by falling back to Feb 28 if needed."""
    try:
        return d.replace(year=d.year + years)
    except ValueError:
        return d.replace(year=d.year + years, day=28)

def fetch_with_retry(url, headers, max_retries=MAX_RETRIES, backoff_base=RETRY_BACKOFF_BASE):
    """
    Fetch URL with exponential backoff for rate limiting and transient errors
    """
    for attempt in range(1, max_retries + 1):
        try:
            # Adding small delay between requests
            time.sleep(REQUEST_DELAY)

            req = urllib.request.Request(url, headers=headers)
            response = urllib.request.urlopen(req, timeout=30)

            if response.getcode() == 200:
                raw_data = response.read().decode('utf-8')
                return json.loads(raw_data)
            else:
                print(f"  Warning: HTTP {response.getcode()} on attempt {attempt}")

        except urllib.error.HTTPError as e:
            if e.code == 429:  # Too Many Requests
                wait_time = backoff_base ** attempt  # Exponential backoff
                print(f"  Rate limited (429) on attempt {attempt}. Waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            elif e.code == 404:
                # Node or page not found - don't retry
                return None
            else:
                print(f"  HTTP error on attempt {attempt}: {e}")

        except Exception as e:
            print(f"  Error on attempt {attempt}: {str(e)}")

        if attempt < max_retries:
            wait_time = backoff_base ** attempt
            print(f"  Waiting {wait_time}s before retry...")
            time.sleep(wait_time)

    print(f"  Failed after {max_retries} attempts")
    return None

def fetch_lmp_da_expost_for_date(target_date):
    """
    Fetch DA ex-post LMP data from MISO API for a specific date and list of target nodes
    :param target_date: specific date to fetch data for (datetime object)
    """
    date_str = target_date.strftime("%Y-%m-%d")
    all_records = []

    headers = {
        'Cache-Control': 'no-cache',
        'Ocp-Apim-Subscription-Key': SUBSCRIPTION_KEY
    }

    # Loop through each node
    for node in TARGET_NODES:
        print(f"  Processing node: {node}")

        page = 1
        max_pages = 10

        while page <= max_pages:
            url = f"{API_BASE}/day-ahead/{date_str}/lmp-expost?node={node}&pageNumber={page}"

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

                record = {
                    'node': item.get('node'),
                    'start_time': time_interval.get('start'),
                    'end_time': time_interval.get('end'),
                    'resolution': time_interval.get('resolution'),
                    'interval_value': time_interval.get('value'),
                    'preliminary_final': item.get('preliminaryFinal'),
                    'lmp': item.get('lmp'),
                    'mcc': item.get('mcc'),  # Marginal Congestion Component
                    'mec': item.get('mec'),  # Marginal Energy Component
                    'mlc': item.get('mlc'),  # Marginal Loss Component
                    'price_type': 'da_expost'
                }
                all_records.append(record)

            # Check if we've reached the last page for this node
            if len(data) < 1000:
                break

            page += 1

    return all_records

def process_date_range(start_date, end_date):
    """
    Process DA LMP for date range and save to Delta table
    :param start_date: start date (datetime object)
    :param end_date: end date (datetime object)
    """
    schema = StructType([
        StructField("node", StringType(), True),
        StructField("start_time", StringType(), True),  # will convert to Timestamp later
        StructField("end_time", StringType(), True),    # will convert to Timestamp later
        StructField("resolution", StringType(), True),
        StructField("interval_value", StringType(), True),
        StructField("preliminary_final", StringType(), True),
        StructField("lmp", DoubleType(), True),
        StructField("mcc", DoubleType(), True),
        StructField("mec", DoubleType(), True),
        StructField("mlc", DoubleType(), True),
        StructField("price_type", StringType(), True),
        StructField("year", StringType(), True),
        StructField("month", StringType(), True),
        StructField("day", StringType(), True)
    ])

    all_records = []
    current_date = start_date

    while current_date <= end_date:
        print(f"Processing date: {current_date.strftime('%Y-%m-%d')}")
        date_records = fetch_lmp_da_expost_for_date(current_date)

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
            (col("lmp").isNotNull()) &
            (col("mcc").isNotNull()) &
            (col("mec").isNotNull()) &
            (col("mlc").isNotNull())
        ).withColumn("lmp_check",
                     (col("mec") + col("mcc") + col("mlc") - col("lmp")).cast(DoubleType()))

        quality_df = quality_df.withColumn("lmp_check_flag",
                                           (spark_abs(col("lmp_check")) > 0.05).cast(IntegerType()))

        mismatch_count = quality_df.filter(col("lmp_check_flag") == 1).count()
        if mismatch_count > 0:
            print(f"  Warning: {mismatch_count} records failed LMP component sum check")

        # Write to Delta table with partitioning
        (quality_df.write
            .format("delta")
            .partitionBy("year", "month", "day")
            .mode("overwrite")
            .save(OUTPUT_DIR))

        print(f"Saved {record_count} DA LMP records to {OUTPUT_DIR}")
        return record_count
    else:
        print("No records to write.")
        return 0

## Main execution
try:
    today = datetime.utcnow().date()
    start_date = today - timedelta(days=1115) # add_years(today, -3)  # 3 years ago
    end_date = today - timedelta(days=1)  # up to yesterday to ensure complete data

    print(f"Starting DA LMP Ex-post data processing from {start_date} to {end_date}...")
    print(f"Target nodes: {', '.join(TARGET_NODES)}")

    record_count = process_date_range(start_date, end_date)

except Exception as e:
    print(f"Error during processing: {str(e)}")
    import traceback
    traceback.print_exc()

finally:
    spark.stop()
    print("Spark session stopped.")
