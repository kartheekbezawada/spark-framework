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
        return df
    
    def process_cd(self, df):
        df = df.drop("md_process_id","md_source_ts","md_created_ts","md_source_path")
        return df
    
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
    
if __name__ == "__main__":
    spark = SparkSession.builder.appName("PayrollDataProcessorApp").getOrCreate()
    processor = PayrollDataProcessor(spark)

    # Load your 'fsd' and 'cd' DataFrames
    fsd_df = processor.read_table("your_fsd_table_path", "alpha")  # Example call
    cd_df = processor.read_table("your_cd_table_path", "alpha")  # Example call

    # Process the data
    result_df = processor.union_for_processing(fsd_df, cd_df)
    result_df.show()
    
    
    
    
        
