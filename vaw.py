from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, row_number, current_date, date_sub
from pyspark.sql.window import Window

class PayrollDataProcessor:
    def __init__(self, spark: SparkSession, key_vault_scope: str, storage_account_name: str):
        self.spark = spark
        self.key_vault_scope = key_vault_scope
        self.storage_account_name = storage_account_name
        self.access_key = dbutils.secrets.get(scope=self.key_vault_scope, key="<your-access-key-name>")
        self.configure_spark = f"fs.azure.account.key.{self.storage_account_name}.dfs.core.windows.net, {self.access_key}"

    def read_delta_table(self, table_path):
        return self.spark.read.format("delta").load(table_path)

    def process_colleague_rates(self, df):
        window_spec = Window.partitionBy("colleague_id", "pay_code", "start_date").orderBy(col("start_date").desc())
        return df.withColumn("rate_seq", row_number().over(window_spec)) \
                 .filter(col("start_date") <= current_date())

    def process_colleague_base_rate(self, df):
        window_spec = Window.partitionBy("colleague_id", "effective_date").orderBy(col("effective_date").desc())
        return df.withColumn("rate_seq", row_number().over(window_spec)) \
                 .filter(col("effective_date") <= current_date())

    def process_colleague_worked_hours(self, df):
        return df.withColumn("datekey", col("work_start_time").cast("date")) \
                 .filter(col("datekey") == date_sub(current_date(), 4))

    def join_and_transform(self, colleague_worked_hours_df, wd_wb_mapping_df, colleague_rates_df, colleague_base_rate_df) -> DataFrame:
        from pyspark.sql.functions import when, lit

        # Join logic similar to SQL query
        # Joining colleague_worked_hours with wd_wb_mapping
        joined_df = colleague_worked_hours_df.alias("c") \
            .join(wd_wb_mapping_df.alias("d"), 
                  (col("c.tcode_name") == col("d.tcode_name")) & 
                  (col("c.htype_name") == col("d.htype_name")) & 
                  (col("c.emp_val4") == col("d.emp_val4")), 
                  "left_outer")

        # Joining with colleague_rates
        joined_df = joined_df.join(colleague_rates_df.alias("a"), 
                                   (col("d.pay_code") == col("a.pay_code")) & 
                                   (col("c.colleague_id") == col("a.colleague_id")) & 
                                   (col("a.rate_seq") == 1), 
                                   "left_outer")

        # Joining with colleague_base_rate
        joined_df = joined_df.join(colleague_base_rate_df.alias("b"), 
                                   (col("c.colleague_id") == col("b.colleague_id").trim()) & 
                                   (col("b.rate_seq") == 1), 
                                   "left_outer")

        # Applying transformation logic as per SQL CASE statements
        joined_df = joined_df.withColumn("premium_rate", 
                                         when(col("d.pay_code") != lit("R010"), 
                                              col("b.basic_hourly_rate").cast("decimal(6,2)") + col("a.value").cast("decimal(6,2)"))
                                         .otherwise(lit(None)))

        joined_df = joined_df.withColumn("basic_rate", 
                                         when(col("d.pay_code") == lit("R010"), 
                                              col("b.basic_hourly_rate"))
                                         .otherwise(lit(None)))

        # Calculated wages logic (simplified for illustration)
        # You'll need to extend this with the full logic from your SQL CASE statements
        joined_df = joined_df.withColumn("calculated_wages", 
                                         when(col("d.pay_code") == lit("R010") & col("d.double_flag").isNull(), 
                                              col("b.basic_hourly_rate").cast("decimal(6,2)") * col("c.wrkd_hrs").cast("decimal(6,2)") * col("c.htype_multiple").cast("decimal(6,2)"))
                                         # Add other conditions here
                                         .otherwise(lit(None)))

        # Return the transformed DataFrame
        return joined_df

    def write_output(self, df, output_path:):
        df.write.format("delta").save(output_path)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("VAW").getOrCreate()
    processor = PayrollDataProcessor(spark, "<your-key-vault-scope>", "<your-storage-account-name>")

    colleague_rates_df = processor.read_delta_table("delta-lake-path/wd_colleague_rates")
    colleague_base_rate_df = processor.read_delta_table("delta-lake-path/wd_colleague_base_rate")
    colleague_worked_hours_df = processor.read_delta_table("delta-lake-path/wb_colleague_hours")
    wd_wb_mapping_df = processor.read_delta_table("delta-lake-path/vaw_wd_wb_mapping")

    # Processing data
    processed_colleague_rates = processor.process_colleague_rates(colleague_rates_df)
    processed_colleague_base_rate = processor.process_colleague_base_rate(colleague_base_rate_df)
    processed_colleague_worked_hours = processor.process_colleague_worked_hours(colleague_worked_hours_df)

    # Writing output
    processor.write_output(processed_colleague_rates, "delta-lake-path/processed_colleague_rates")
    