from pyspark.sql import SparkSession
from pyspark.sql import functions as F

class PayrollDataProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.alpha_account_name = "your_storage_account_name"
        self.alpha_account_key = "your_storage_account_key"
        self.alpha_container_name = "your_storage_container_name"
        self.alpha_storage_config = {
            "fs.azure.account.key." + self.alpha_account_name + ".blob.core.windows.net": self.alpha_account_key
        }
        self.alpha_storage_url = f"wasbs://{self.alpha_container_name}@{self.alpha_account_name}.blob.core.windows.net"
        self.set_storage_configuration()

    def set_storage_configuration(self):
        for config, value in self.alpha_storage_config.items():
            self.spark.conf.set(config, value)

    def read_table(self, table_path):
        full_path = f"{self.alpha_storage_url}/{table_path}"
        df = self.spark.read.format("delta").load(full_path)  # Adjust the format as necessary
        return self.drop_columns(df)

    def drop_columns(self, df):
        columns_to_drop = ["md_process_id", "md_source_ts", "md_created_ts", "md_source_path"]
        return df.drop(*columns_to_drop)

    def write_table(self, df, table_path, mode="overwrite"):
        full_path = f"{self.alpha_storage_url}/{table_path}"
        df.write.format("delta").mode(mode).save(full_path)
    
    def convert_sql_to_pyspark(self):
   
        fsd = self.read_table(fsd_path)
        cd = self.read_table(cd_path)
        s = self.read_table(s_path)
        v = self.read_table(v_path)
        
        current_date = F.current_date()

        # Mimicking the SQL WHERE clause for fsd and cd
        join_condition_fsd_cd = F.col("fsd.summary_date") == F.col("cd.calendar_date")
        
        # Subquery equivalent in PySpark for checking wm_week, calendar_year, and calendar_date
        cd_filtered = cd.filter(F.col("cd.calendar_date") == current_date)
        
        # First part of the UNION ALL
        first_part = fsd.join(cd, join_condition_fsd_cd) \
                        .filter(cd["wm_week"].isin(cd_filtered.select("wm_week").distinct().rdd.flatMap(lambda x: x).collect())) \
                        .groupBy("fsd.store_nbr", "fsd.dept_nbr", "cd.wm_week") \
                        .agg(
                            F.lit(current_date).alias("VAW_wtd_date"),
                            F.sum("fsd.sales_retail_amt").alias("total_sales")
                        )

        # Second part of the UNION ALL
        second_part = s.join(v, s["visit_nbr"] == v["visit_nbr"]) \
                       .join(cd, v["visit_date"] == cd["calendar_date"]) \
                       .filter((v["register_nbr"] == 80) & (s["other_income_ind"].isNull()) &
                               cd["wm_week"].isin(cd_filtered.select("wm_week").distinct().rdd.flatMap(lambda x: x).collect())) \
                       .groupBy("s.store_nbr", "cd.wm_week") \
                       .agg(
                           F.lit(current_date).alias("VAW_wtd_date"),
                           F.lit(668).alias("dept_nbr"),
                           F.sum("s.retail").alias("total_sales")
                       )

        # Union the two parts
        result_df = first_part.unionAll(second_part)

        # Assuming result_path is the path where you want to write the result
        result_path = "path/to/your/result/table"
        self.write_table(result_df, result_path)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PayrollDataProcessorApp").getOrCreate()

    processor = PayrollDataProcessor(spark)
       
    fsd_path = "path/to/your/fsd/table"
    cd_path = "path/to/your/dim_calendar_day/table"
    s_path = "path/to/your/vw_Scan/table"
    v_path = "path/to/your/vw_visit/table"
        
    # Process the data
    processor.convert_sql_to_pyspark()
