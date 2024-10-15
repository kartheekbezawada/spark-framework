from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ETL Process for Delta Lake") \
    .getOrCreate()

# Load source Delta table
source_df = spark.read.format("delta").load("coreprod.infomart.fact_user_bp_article_item_supp")

# Define the path for the destination table
destination_table_path = "coreprod.infomart.fact_user_bp_article_item_supp2"

# Try to load the destination DataFrame, create if it doesn't exist
try:
    destination_df = spark.read.format("delta").load(destination_table_path)
except Exception:
    print("Destination table does not exist. Creating a new one.")
    # Create an empty DataFrame with the same schema as the source
    destination_df = spark.createDataFrame([], source_df.schema)

# Get the current date for end date updates
current_date = datetime.now().date()

# Prepare new articles and updates
updates_df = source_df.alias("src") \
    .join(destination_df.alias("dst"), "article_sk", "left") \
    .select(
        F.coalesce(F.col("src.business_partner_sk"), F.col("dst.business_partner_sk")).alias("business_partner_sk"),
        F.coalesce(F.col("src.b_user_principal_name"), F.col("dst.b_user_principal_name")).alias("b_user_principal_name"),
        F.col("src.article_sk"),
        F.when(F.isnull("dst.article_sk"), F.lit("1900-01-01")).otherwise(F.col("dst.start_date")).alias("start_date"),
        F.when(F.isnull("dst.article_sk"), F.lit("9999-12-31")).otherwise(F.when(F.col("dst.article_sk").isNotNull(), F.lit(current_date - timedelta(days=1))).otherwise(F.col("dst.end_date"))).alias("end_date"),
        F.col("src.md_creation_date"),
        F.col("src.md_created_ts"),
        F.col("src.md_process_id")
    )

# Perform merge operation
updates_df.createOrReplaceTempView("updates")

spark.sql(f"""
MERGE INTO delta.`{destination_table_path}` AS dst
USING updates AS src
ON dst.article_sk = src.article_sk
WHEN MATCHED AND dst.article_sk IS NOT NULL THEN
    UPDATE SET 
        dst.end_date = src.end_date,
        dst.start_date = src.start_date,
        dst.md_creation_date = src.md_creation_date,
        dst.md_created_ts = src.md_created_ts,
        dst.md_process_id = src.md_process_id,
        dst.business_partner_sk = src.business_partner_sk,
        dst.b_user_principal_name = src.b_user_principal_name
WHEN NOT MATCHED THEN
    INSERT (business_partner_sk, b_user_principal_name, article_sk, start_date, end_date, md_creation_date, md_created_ts, md_process_id)
    VALUES (src.business_partner_sk, src.b_user_principal_name, src.article_sk, src.start_date, src.end_date, src.md_creation_date, src.md_created_ts, src.md_process_id)
""")

# Stop Spark session
spark.stop()
