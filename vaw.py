from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, row_number, current_date, date_sub
from pyspark.sql.window import Window

class PayrollDataProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.apha_account_name = "storage_account_name"
        self.apha_account_key = "storage_account_key"
        self.apha_container_name = "storage_container_name"
        self.apha_storage_config = {"fs.azure.account.key." + self.apha_account_name + ".blob.core.windows.net": self.apha_account_key}
        self.apha_storage_url = f"wasbs://{self.apha_container_name}@{self.apha_account_name}.blob.core.windows.net"
        
    def read_delta_table(self, path):
        table_path = f"{self.apha_storage_url}/{path}"
        return self.spark.read.options(**self.apha_storage_config).format("delta").load(table_path)

    def prefix_columns(self, df, prefix):
        for col_name in df.columns:
            df = df.withColumnRenamed(col_name, f"{prefix}_{col_name}")
        return df

    def process_colleague_rates(self, df):
        window_spec = Window.partitionBy("colleague_id", "pay_code", "start_date").orderBy(col("start_date").desc())
        processed_df = df.drop("md_process_id","md_source_ts","md_created_ts","md_source_path") \
                         .withColumn("rate_seq", row_number().over(window_spec)) \
                         .filter(col("start_date") <= current_date())
        return self.prefix_columns(processed_df, "cr")

    def process_colleague_base_rate(self, df):
        window_spec = Window.partitionBy("colleague_id", "effective_date").orderBy(col("effective_date").desc())
        processed_df = df.drop("md_process_id","md_source_ts","md_created_ts","md_source_path") \
                         .withColumn("rate_seq", row_number().over(window_spec)) \
                         .filter(col("effective_date") <= current_date())
        return self.prefix_columns(processed_df, "cbr")

    def process_colleague_worked_hours(self, df):
        processed_df = df.drop("md_process_id","md_source_ts","md_created_ts","md_source_path") \
                         .withColumn("datekey", col("work_start_time").cast("date")) \
                         .filter(col("datekey") == date_sub(current_date(), 4))
        return self.prefix_columns(processed_df, "cwh")

    def process_wd_wb_mapping(self, df):
        processed_df = df.drop("md_process_id","md_source_ts","md_created_ts","md_source_path")
        return self.prefix_columns(processed_df, "wdwbmap")

    def join_df(self, cwh_df, wdwbmap_df, cr_df, cbr_df):
        joined_df = cwh_df.join(wdwbmap_df, 
                                (col("cwh_tcode_name") == col("wdwbmap_tcode_name")) & 
                                (col("cwh_htype_name") == col("wdwbmap_htype_name")) & 
                                (col("cwh_emp_val4") == col("wdwbmap_emp_val4")), 
                                "left_outer")

        joined_df = joined_df.join(cr_df, 
                                   (col("cwh_pay_code") == col("cr_pay_code")) & 
                                   (col("cwh_colleague_id") == col("cr_colleague_id")) & 
                                   (col("cr_rate_seq") == 1), 
                                   "left_outer")

        joined_df = joined_df.join(cbr_df, 
                                   (col("cwh_colleague_id") == col("cbr_colleague_id")) & 
                                   (col("cbr_rate_seq") == 1), 
                                   "left_outer")

        return joined_df

    def transform_df(self, df):
        pass  # Implement any additional transformations if needed

if __name__ == "__main__":
    spark = SparkSession.builder.appName("VAW").getOrCreate()
    processor = PayrollDataProcessor(spark)

    colleague_rates_df = processor.read_delta_table("delta_lake_path/wd_colleague_rates")
    colleague_base_rate_df = processor.read_delta_table("delta_lake_path/wd_colleague_base_rate")
    colleague_worked_hours_df = processor.read_delta_table("delta_lake_path/wb_colleague_hours")
    wd_wb_mapping_df = processor.read_delta_table("delta_lake_path/vaw_wd_wb_mapping")

    pcr_df = processor.process_colleague_rates(colleague_rates_df)
    pcbr_df = processor
