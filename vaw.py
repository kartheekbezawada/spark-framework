from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, row_number, current_date, date_sub, substring, when
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
        processed_df = (df.drop("md_process_id", "md_source_ts", "md_created_ts", "md_source_path")
                         .filter(col("datekey") == date_sub(current_date(), 4))
                         .withColumn("datekey", col("work_start_time").cast("date"))
                         .withColumn("store_number", substring(col("dock_name"), 1, 4))
                         .withColumn("division_number", substring(col("dock_name"), 5, 8))
                        )
        return self.prefix_columns(processed_df, "cwh")

    def process_wd_wb_mapping(self, df):
        processed_df = df.drop("md_process_id","md_source_ts","md_created_ts","md_source_path")
        return self.prefix_columns(processed_df, "wdwbmap")

    def process_div_cc_hierarchy(self, df):
        processed_df = df.drop("md_process_id","md_source_ts","md_created_ts","md_source_path")
        return self.prefix_columns(processed_df, "dcch")

    def join_df(self, process_colleague_worked_hours, process_wd_wb_mapping, process_colleague_rates, process_colleague_base_rate):
        joined_df = process_colleague_worked_hours.join(process_wd_wb_mapping,
                        (col("cwh_tcode_name") == col("wdwbmap_tcode_name")) & 
                        (col("cwh_htype_name") == col("wdwbmap_htype_name")) & 
                        (col("cwh_emp_val4") == col("wdwbmap_emp_val4")), 
                        "left_outer")

        joined_df = joined_df.join(process_colleague_rates, 
                       (col("cwh_pay_code") == col("cr_pay_code")) & 
                       (col("cwh_colleague_id") == col("cr_colleague_id")) & 
                       (col("cr_rate_seq") == 1), 
                       "left_outer")

        joined_df = joined_df.join(process_colleague_base_rate, 
                       (col("cwh_colleague_id") == col("cbr_colleague_id")) & 
                       (col("cbr_rate_seq") == 1), 
                       "left_outer")

        return joined_df


    def select_and_rename_columns(self, df):
        # Select and rename the required columns
        return df.select(
            "cwh_dock_name",
            "cwh_emp_name",
            "cwh_datekey",
            "cwh_wrkd_starttime",
            "cwh_wrkd_endtime",
            "cwh_wrks_hrs",
            "cwh_minor",
            "cwh_seasonal",
            "cwh_tcodename",
            "cwh_htypename",
            "cwh_htype_multiple",
            "cwh_emp_val14",
            "wdwbmap_paycode",
            "wdwbmap_double_flag",
            "cbr_basic_hourly_rate"
        )

    def add_composite_key(self, df):
        # Add a composite key to the DataFrame
        df = df.withColumn("composite_key", 
                             F.concat("-",F.col("cwh_dock_name"),F.col("cwh_emp_name"),F.col("cwh_datekey"), F.col(htype_name), F.col("tcode_name")))
        return df
    
    def apply_case_statement(self, df):
        # Apply the case statement logic with multiple conditions
        return df.withColumn(
            "calculated_wages",
            when(
                (col("wdwbmap_paycode") == "R010") & (col("wdwbmap_double_flag").isNull()),
                col("cbr_basic_hourly_rate").cast("decimal(6,2)") * 
                col("cwh_wrkd_hrs").cast("decimal(6,2)") * 
                col("cwh_htype_multiple").cast("decimal(6,2)")
            ).when(
                (col("wdwbmap_paycode") == "R010") & (col("wdwbmap_double_flag") == "y"),
                2 * (col("cbr_basic_hourly_rate").cast("decimal(6,2)") *
                col("cwh_wrkd_hrs").cast("decimal(6,2)"))
            ).when(
                (col("wdwbmap_paycode") != "R010") & (col("wdwbmap_double_flag").isNull()),
                (col("cbr_basic_hourly_rate").cast("decimal(6,2)") + col("cr_value").cast("decimal(6,2)")) *
                col("cwh_wrkd_hrs").cast("decimal(6,2)")
            ).when(
                (col("wdwbmap_paycode") != "R010") & (col("wdwbmap_double_flag") == "y"),
                2 * (col("cbr_basic_hourly_rate").cast("decimal(6,2)") + col("cr_value").cast("decimal(6,2)")) *
                col("cwh_wrkd_hrs").cast("decimal(6,2)")
            ).otherwise(None)
        )


    def process_and_transform_data(self, rates_path, base_rate_path, worked_hours_path, mapping_path):
        # Reading data from Delta Lake tables
        colleague_rates_df = self.read_delta_table(rates_path)
        colleague_base_rate_df = self.read_delta_table(base_rate_path)
        colleague_worked_hours_df = self.read_delta_table(worked_hours_path)
        wd_wb_mapping_df = self.read_delta_table(mapping_path)

        # Processing data
        pcr = self.process_colleague_rates(colleague_rates_df)
        pcbr = self.process_colleague_base_rate(colleague_base_rate_df)
        pcwhrs = self.process_colleague_worked_hours(colleague_worked_hours_df)
        pwdwbm = self.process_wd_wb_mapping(wd_wb_mapping_df)
        
        # Joining DataFrames
        joined_df = self.join_df(pcwhrs, pwdwbm, pcr, pcbr)
        # Transforming DataFrames
        df_with_columns = self.select_and_rename_columns(joined_df)
        composite_key_df = self.add_composite_key(df_with_columns)
        transformed_df = self.apply_case_statement(composite_key_df)
       

        return transformed_df
    
    def write_delta_table(self, df, path):
        # Extract year and month from the date column for partitioning
        df = df.withColumn("year", year(col("date"))) \
               .withColumn("month", month(col("date")))

        # Define the path for the Delta table
        delta_path = f"{self.apha_storage_url}/{path}"

        # Write the DataFrame as a Delta table
        df.write.format("delta") \
                .mode("append") \
                .partitionBy("year", "month") \
                .save(delta_path)
                
        
if __name__ == "__main__":
    spark = SparkSession.builder.appName("PayrollDataProcessorApp").getOrCreate()
                        
    processor = PayrollDataProcessor(spark)
    
    # Access alpha_account_name and alpha_account_key from the processor instance
    alpha_account_name = processor.alpha_account_name
    alpha_account_key = processor.alpha_account_key

    # If you need to use them for further Spark session configuration, do it here
    # For example, if you need to reconfigure the Spark session with these details (though typically you'd set these at SparkSession creation)
    spark.conf.set(f"fs.azure.account.key.{alpha_account_name}.blob.core.windows.net", alpha_account_key)

    # Proceed with processing
    transformed_df = processor.process_and_transform_data(
        "delta_lake_path/wd_colleague_rates",
        "delta_lake_path/wd_colleague_base_rate",
        "delta_lake_path/wb_colleague_hours",
        "delta_lake_path/vaw_wd_wb_mapping"
    )

    # Example of showing the transformed DataFrame
    transformed_df.show()
    # Example of writing the transformed DataFrame
    processor.write_delta_table(transformed_df, "path/to/delta_table")