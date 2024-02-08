from pyspark.sql import SparkSession
from pyspark.sql import functions 
import os
import datetime as dt

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
        
        self.beta_account_name = "your_storage_account_name"
        self.beta_account_key = "your_storage_account_key"
        self.beta_container_name = "your_storage_container_name"
        self.beta_storage_config = {
            "fs.azure.account.key." + self.beta_account_name + ".blob.core.windows.net": self.beta_account_key
        }
        self.beta_storage_url = f"wasbs://{self.beta_container_name}@{self.beta_account_name}.blob.core.windows.net"
        
        self.charlie_account_name = "your_storage_account_name"
        self.charlie_account_key = "your_storage_account_key"
        self.charlie_container_name = "your_storage_container_name"
        self.charlie_storage_config = {
            "fs.azure.account.key." + self.charlie_account_name + ".blob.core.windows.net": self.charlie_account_key
        }
        self.charlie_storage_url = f"wasbs://{self.charlie_container_name}@{self.charlie_account_name}.blob.core.windows.net"
        
        self.set_storage_configuration()

    def set_storage_configuration(self):
        # Sets the Azure Blob Storage configuration on the Spark session
        self.spark.conf.set(f"fs.azure.account.key.{self.alpha_account_name}.blob.core.windows.net", self.alpha_account_key)
        self.spark.conf.set(f"fs.azure.account.key.{self.beta_account_name}.blob.core.windows.net", self.beta_account_key)
        self.spark.conf.set(f"fs.azure.account.key.{self.charlie_account_name}.blob.core.windows.net", self.charlie_account_key)
    
    def prefix_columns(self, df, prefix):
        # This method remains the same, it renames columns by adding a prefix.
        for col_name in df.columns:
            df = df.withColumnRenamed(col_name, f"{prefix}_{col_name}")
        return df

    def alpha_read_table(self, table_path):
        full_path = f"{self.alpha_storage_url}/{table_path}"
        df = self.spark.read.format("delta").load(full_path)  # Adjust the format as necessary
        return self.drop_columns(df)
    
    def beta_read_table(self, table_path):
        full_path = f"{self.beta_storage_url}/{table_path}"
        df = self.spark.read.format("delta").load(full_path)
        return self.drop_columns(df)
    
    def charlie_read_table(self, table_path):
        full_path = f"{self.charlie_storage_url}/{table_path}"
        df = self.spark.read.format("delta").load(full_path)
        return self.drop_columns(df)
    
    
    # get only the columns that are needed, column names are store_nbr, dept_nbr, summary_date, sales_retail_amount and drop the rest
    def process_fsd(self, df):
        df = df.select("store_nbr", "dept_nbr", "summary_date", "sales_retail_amount")
        return self.prefix_columns(df, "fsd")
    
    def process_cd(self, df):
        df = df.drop("md_process_id","md_source_ts","md_created_ts","md_source_path")
        return self.prefix_columns(df, "cd")
    
    def process_scan(self, df):
        df = df.select("store_nbr", "scan_dept_nbr", "retail_price", "other_income_ind","visit_nbr", "visit_date")
        return self.prefix_columns(df, "scan")
    
    def process_visit(self, df):
        df = df.select( "visit_nbr","store_nbr", "visit_date", "register_nbr")
        return self.prefix_columns(df, "visit")
    
    def union_for_processing(self, fsd_df, cd_df):
        # Assuming 'summary_date' in 'fsd' and 'calendar_date' in 'cd' are already in date format or compatible string format.
        # If not, you might need to convert them using to_date() with the appropriate format.
        
        # Filter 'cd' to include only entries for the current date
        current_date_df = cd_df.filter(to_date(col('calendar_date'), 'dd/MM/yyyy') == to_date(lit(date_format(current_date(), 'dd/MM/yyyy')), 'dd/MM/yyyy'))

        # Join 'fsd' with 'cd' to filter records matching the current week based on 'cd'
        join_condition = to_date(fsd_df['summary_date'], 'dd/MM/yyyy') == to_date(cd_df['calendar_date'], 'dd/MM/yyyy')
        filtered_fsd = fsd_df.join(cd_df, join_condition)

        # Filter 'filtered_fsd' further based on the existence of matching 'wm_week' and 'calendar_year' in 'current_date_df'
        # This replicates the EXISTS logic by ensuring 'filtered_fsd' only includes rows for the current week and year.
        current_week_df = filtered_fsd.join(current_date_df, (filtered_fsd['wm_week'] == current_date_df['wm_week']) & (filtered_fsd['calendar_year'] == current_date_df['calendar_year']), 'inner')

        # Aggregate to calculate total_sales
        result_df = current_week_df.groupBy('store_nbr', 'dept_nbr', 'wm_week').agg(
            lit(date_format(current_date(), 'dd/MM/yyyy')).alias('VAW_wtd_date'),
            _sum('sales_retail_amt').alias('total_sales')
        ).select('VAW_wtd_date', 'store_nbr', 'dept_nbr', 'wm_week', 'total_sales')

        return result_df
    
    def union_two_processing(self, scan_df, visit_df, calendar_df):
        # Assume scan_df, visit_df, and calendar_df are already read, processed, and passed to this method
        
        # Filter calendar_df for the current date and extract distinct weeks and years
        current_week_and_year = calendar_df.filter(
            to_date(col('cd_calendar_date'), 'dd/MM/yyyy') == current_date()
        ).select('cd_wm_week', 'cd_calendar_year').distinct()

        # Join scan_df with visit_df based on matching conditions
        joined_df = scan_df.join(
            visit_df,
            (scan_df["scan_visit_nbr"] == visit_df["visit_visit_nbr"]) &
            (scan_df["scan_store_nbr"] == visit_df["visit_store_nbr"]) &
            (to_date(scan_df["scan_visit_date"], 'dd/MM/yyyy') == to_date(visit_df["visit_visit_date"], 'dd/MM/yyyy')) &
            (visit_df["visit_register_nbr"] == 80) &
            scan_df["scan_other_income_ind"].isNull(),
            "inner"
        )

        # Then join the result with calendar_df
        joined_df = joined_df.join(
            calendar_df,
            to_date(joined_df["scan_visit_date"], 'dd/MM/yyyy') == calendar_df["cd_calendar_date"],
            "inner"
        )

        # Apply the filter based on current week and year
        final_df = joined_df.join(
            current_week_and_year,
            (joined_df["cd_wm_week"] == current_week_and_year["cd_wm_week"]) &
            (joined_df["cd_calendar_year"] == current_week_and_year["cd_calendar_year"]),
            "inner"
        )

        # Aggregate to calculate total sales
        result_df = final_df.groupBy("scan_store_nbr", "cd_wm_week").agg(
            lit(date_format(current_date(), 'dd/MM/yyyy')).alias('VAW_wtd_date'),
            lit(668).alias('dept_nbr'),  # Assuming dept_nbr is constant
            Fsum("scan_retail").alias("total_sales")
        ).select("VAW_wtd_date", "scan_store_nbr", "dept_nbr", "cd_wm_week", "total_sales")

        return result_df
    
    
if __name__ == "__main__":
    spark = SparkSession.builder.appName("PayrollDataProcessorApp").getOrCreate()
    processor = PayrollDataProcessor(spark)

    # Load your 'fsd' and 'cd' DataFrames
    fsd_df = processor.read_table("your_fsd_table_path", "alpha")  # Example call
    cd_df = processor.read_table("your_cd_table_path", "alpha")  # Example call

    # Process the data
    result_df = processor.union_for_processing(fsd_df, cd_df)
    result_df.show()
    
    
    
    
        
def union_for_processing(self, fsd_path, cd_path):
        fsd_df = self.read_table(fsd_path, "alpha")  # Assuming fsd data is in 'alpha' account
        cd_df = self.read_table(cd_path, "alpha")  # Assuming cd data is also in 'alpha' account

        fsd_df = self.process_fsd(fsd_df)
        cd_df = self.process_cd(cd_df)

        # Assuming 'summary_date' in 'fsd_df' and 'calendar_date' in 'cd_df' are already in date format.
        current_date_df = cd_df.filter(cd_df['cd_calendar_date'] == current_date())

        # Join fsd_df with cd_df
        joined_df = fsd_df.join(cd_df, fsd_df['fsd_summary_date'] == cd_df['cd_calendar_date'])

        # Aggregation
        result_df = joined_df.groupBy('fsd_store_nbr', 'fsd_dept_nbr', 'cd_wm_week').agg(
            lit(date_format(current_date(), 'dd/MM/yyyy')).alias('VAW_wtd_date'),
            Fsum('fsd_sales_retail_amount').alias('total_sales')
        )

        return result_df
    
