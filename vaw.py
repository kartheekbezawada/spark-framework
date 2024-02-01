from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, row_number, current_date, date_sub
from pyspark.sql.functions import when, lit
from pyspark.sql.window import Window

class PayrollDataProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.apha_account_name = "storage_account_name"
        self.apha_account_key = "storage_account_key"
        self.apha_container_name = "storage_container_name"
        self.apha_storage_config = {"fs.azure.account.key." + self.apha_account_name + ".blob.core.windows.net: " + self.apha_account_key}
        self.apha_storage_url = f"wasbs://{self.apha_container_name}@{self.apha_account_name}.blob.core.windows.net"
        
    def read_delta_table(self, path):
        table_path = f"{self.apha_storage_url}/{path}"
        return self.spark.read.opotions(**self.apha_storage_config).format("delta").load(table_path)
     

    def process_colleague_rates(self, df):
        window_spec = Window.partitionBy("colleague_id", "pay_code", "start_date").orderBy(col("start_date").desc())
        return df.drop("md_process_id","md_source_ts","md_created_ts","md_source_path").withColumn("rate_seq", row_number().over(window_spec)) \
                 .filter(col("start_date") <= current_date())

    def process_colleague_base_rate(self, df):
        window_spec = Window.partitionBy("colleague_id", "effective_date").orderBy(col("effective_date").desc())
        return df.drop("md_process_id","md_source_ts","md_created_ts","md_source_path").withColumn("rate_seq", row_number().over(window_spec)) \
                 .filter(col("effective_date") <= current_date())

    def process_colleague_worked_hours(self, df):
        return df.drop("md_process_id","md_source_ts","md_created_ts","md_source_path").withColumn("datekey", col("work_start_time").cast("date")) \
                 .filter(col("datekey") == date_sub(current_date(), 4))
                 
    def process_sales_div_mapping(self, df):
        return df.drop("md_process_id","md_source_ts","md_created_ts","md_source_path")
    
    def process_wd_wb_mapping(self, df):
        return df.drop("md_process_id","md_source_ts","md_created_ts","md_source_path")
    

    def join_df(self, process_colleague_worked_hours, process_wd_wb_mapping, process_colleague_rates, process_colleague_base_rate):
        # Prefixing columns with table names
        def prefix_columns(df, prefix):
            for col_name in df.columns:
                df = df.withColumnRenamed(col_name, f"{prefix}_{col_name}")
            return df

        # Applying prefixes to each DataFrame
        p_cwh_prefixed = prefix_columns(process_colleague_worked_hours, "cwh")
        p_wd_wb_map_prefixed = prefix_columns(process_wd_wb_mapping, "wdwbmap")
        p_cr_prefixed = prefix_columns(process_colleague_rates, "cr")
        p_cbr_prefixed = prefix_columns(process_colleague_base_rate, "cbr")

        # Join logic with prefixed column names
        joined_df = p_cwh_prefixed.alias("p_cwh") \
            .join(p_wd_wb_map_prefixed.alias("p_wd_wb_map"), 
                  (col("p_cwh.cwh_tcode_name") == col("p_wd_wb_map.wdwbmap_tcode_name")) & 
                  (col("p_cwh.cwh_htype_name") == col("p_wd_wb_map.wdwbmap_htype_name")) & 
                  (col("p_cwh.cwh_emp_val4") == col("p_wd_wb_map.wdwbmap_emp_val4")), 
                  "left_outer")

        joined_df = joined_df.alias("a").join(p_cr_prefixed.alias("p_cr"), 
                                   (col("a.cwh_pay_code") == col("p_cr.cr_pay_code")) & 
                                   (col("a.cwh_colleague_id") == col("p_cr.cr_colleague_id")) & 
                                   (col("p_cr.cr_rate_seq") == 1), 
                                   "left_outer")

        joined_df = joined_df.alias("b").join(p_cbr_prefixed.alias("p_cbr"), 
                                   (col("b.cwh_colleague_id") == col("p_cbr.cbr_colleague_id").trim()) & 
                                   (col("p_cbr.cbr_rate_seq") == 1), 
                                   "left_outer")

        # Return the transformed DataFrame
        return joined_df

    def transform_df(self,df):
        pass

if __name__ == "__main__":
    spark = SparkSession.builder.appName("VAW").getOrCreate()
    processor = PayrollDataProcessor(spark)

    colleague_rates_path = processor.read_delta_table("delta_lake_path/wd_colleague_rates")
    colleague_base_rate_path = processor.read_delta_table("delta_lake_path/wd_colleague_base_rate")
    colleague_worked_hours_path = processor.read_delta_table("delta_lake_path/wb_colleague_hours")
    wd_wb_mapping_path = processor.read_delta_table("delta_lake_path/vaw_wd_wb_mapping")

    # Processing data
    pcr = processor.process_colleague_rates(colleague_rates_path)
    pcbr = processor.process_colleague_base_rate(colleague_base_rate_path)
    pcwhrs = processor.process_colleague_worked_hours(colleague_worked_hours_path)
    pwdwb_map = processor.process_wd_wb_mapping(wd_wb_mapping_path)


    