from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, date_format, when

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
    def read_wkly_planner(self, df):
        df = df.drop("md_process_id","md_source_ts","md_created_ts","md_source_path")
        return df
    
    def process_snapshot(self, df):
        df = df.drop("md_process_id","md_source_ts","md_created_ts","md_source_path")
        return self.prefix_columns(df, "ss")
    
    def process_ref(self, df):
        df = df.drop("md_process_id","md_source_ts","md_created_ts","md_source_path")
        return self.prefix_columns(df, "ref1")
    
    def process_ref2(self, df):
        df = df.select("store_nbr", "scan_dept_nbr", "retail_price", "other_income_ind","visit_nbr", "visit_date")
        return self.prefix_columns(df, "ref2")
    
    
    def process_wkly_planner(self, df):
        df_transformed = df \
            .withColumn("Today_Date", F.current_date()) \
            .withColumn("Week_Day", F.date_format(F.current_date(), 'EEEE')) \
            .withColumn("FS_Sat_Sales", df["wkly_sales"] / 100 * df["sat_sales"]) \
            .withColumn("FS_Sun_Sales", df["wkly_sales"] / 100 * df["sun_sales"]) \
            .withColumn("FS_Mon_Sales", df["wkly_sales"] / 100 * df["mon_sales"]) \
            .withColumn("FS_Tue_Sales", df["wkly_sales"] / 100 * df["tue_sales"]) \
            .withColumn("FS_Wed_Sales", df["wkly_sales"] / 100 * df["wed_sales"]) \
            .withColumn("FS_Thu_Sales", df["wkly_sales"] / 100 * df["thu_sales"]) \
            .withColumn("FS_Fri_Sales", df["wkly_sales"] / 100 * df["fri_sales"]) \
            .withColumn("FH_Sat_Hrs", df["wkly_hrs"] / 100 * df["sat_hours"]) \
            .withColumn("FH_Sun_Hrs", df["wkly_hrs"] / 100 * df["sun_hours"]) \
            .withColumn("FH_Mon_Hrs", df["wkly_hrs"] / 100 * df["mon_hours"]) \
            .withColumn("FH_Tue_Hrs", df["wkly_hrs"] / 100 * df["tue_hours"]) \
            .withColumn("FH_Wed_Hrs", df["wkly_hrs"] / 100 * df["wed_hours"]) \
            .withColumn("FH_Thu_Hrs", df["wkly_hrs"] / 100 * df["thu_hours"]) \
            .withColumn("FH_Fri_Hrs", df["wkly_hrs"] / 100 * df["fri_hours"]) \
            .withColumn("FC_Sat_Cost", df["wkly_cost"] / 100 * df["sat_cost"]) \
            .withColumn("FC_Sun_Cost", df["wkly_cost"] / 100 * df["sun_cost"]) \
            .withColumn("FC_Mon_Cost", df["wkly_cost"] / 100 * df["mon_cost"]) \
            .withColumn("FC_Tue_Cost", df["wkly_cost"] / 100 * df["tue_cost"]) \
            .withColumn("FC_Wed_Cost", df["wkly_cost"] / 100 * df["wed_cost"]) \
            .withColumn("FC_Thu_Cost", df["wkly_cost"] / 100 * df["thu_cost"]) \
            .withColumn("FC_Fri_Cost", df["wkly_cost"] / 100 * df["fri_cost"])
    return df_transformed


    def process_previous_day_metrics(self, df):
        # Adding Today_Date and Week_Day columns for reference
        df = df.select("Store_nbr","Division", "Today_Date", "Week_Day", "year","Week_Nbr","FS_Weekly_Sales","FS_Weekly_Hrs","FS_Wkly_Cost","FS_Wkly_Cost_Hour")
        # Calculating Previous Day Sales, Hours, Cost, and CPH
        df = df.withColumn("Previous_Day_Sales",
                           when(df["Week_Day"] == 'Saturday', df["FS_Fri_Sales"])
                           .when(df["Week_Day"] == 'Sunday', df["FS_Sat_Sales"])
                           .when(df["Week_Day"] == 'Monday', df["FS_Sun_Sales"])
                           .when(df["Week_Day"] == 'Tuesday', df["FS_Mon_Sales"])
                           .when(df["Week_Day"] == 'Wednesday', df["FS_Tue_Sales"])
                           .when(df["Week_Day"] == 'Thursday', df["FS_Wed_Sales"])
                           .when(df["Week_Day"] == 'Friday', df["FS_Thu_Sales"]))
        
        df = df.withColumn("Previous_Day_Hrs",
                           when(df["Week_Day"] == 'Saturday', df["FH_Fri_Hrs"])
                           .when(df["Week_Day"] == 'Sunday', df["FH_Sat_Hrs"])
                           .when(df["Week_Day"] == 'Monday', df["FH_Sun_Hrs"])
                           .when(df["Week_Day"] == 'Tuesday', df["FH_Mon_Hrs"])
                           .when(df["Week_Day"] == 'Wednesday', df["FH_Tue_Hrs"])
                           .when(df["Week_Day"] == 'Thursday', df["FH_Wed_Hrs"])
                           .when(df["Week_Day"] == 'Friday', df["FH_Thu_Hrs"]))
        
        df = df.withColumn("Previous_Day_Cost",
                           when(df["Week_Day"] == 'Saturday', df["FC_Fri_Cost"])
                           .when(df["Week_Day"] == 'Sunday', df["FC_Sat_Cost"])
                           .when(df["Week_Day"] == 'Monday', df["FC_Sun_Cost"])
                           .when(df["Week_Day"] == 'Tuesday', df["FC_Mon_Cost"])
                           .when(df["Week_Day"] == 'Wednesday', df["FC_Tue_Cost"])
                           .when(df["Week_Day"] == 'Thursday', df["FC_Wed_Cost"])
                           .when(df["Week_Day"] == 'Friday', df["FC_Thu_Cost"]))

        df = df.withColumn("Previous_Day_CPH",
                           when(df["Week_Day"] == 'Saturday', df["Fri_Cph"])
                           .when(df["Week_Day"] == 'Sunday', df["Sat_Cph"])
                           .when(df["Week_Day"] == 'Monday', df["Sun_Cph"])
                           .when(df["Week_Day"] == 'Tuesday', df["Mon_Cph"])
                           .when(df["Week_Day"] == 'Wednesday', df["Tue_Cph"])
                           .when(df["Week_Day"] == 'Thursday', df["Wed_Cph"])
                           .when(df["Week_Day"] == 'Friday', df["Thu_Cph"]))
        return df

    # Join snap shot with ref tables 
    
    def join_ss_ref(self, df_snapshot, df_ref, df_ref2):
        # Assuming df_snapshot, df_ref, and df_ref2 are DataFrames 
        # produced by process_snapshot, process_ref, and process_ref2 methods respectively.

        # Register temporary views for the DataFrames
        df_snapshot.createOrReplaceTempView("ss")
        df_ref.createOrReplaceTempView("ref")
        df_ref2.createOrReplaceTempView("ref2")

        # Perform the join using Spark SQL
        join_query = """
            SELECT 
                ss.store_nbr AS ss_store_nbr,
                ss.VAW_wtd_date AS ss_VAW_wtd_date,
                ref.dept_nbr AS ref_dept_nbr,
                ref.description AS ref_description,
                ref.division AS ref_division,
                ss.cd_wm_week AS ss_cd_wm_week,
                ss.total_sales AS ss_total_sales,
                ref2.division AS ref2_division,
                ref2.description AS ref2_description
            FROM ss
            LEFT JOIN ref ON ss.dept_nbr = ref.dept_nbr
            LEFT JOIN ref2 ON ss.division = ref2.division
        """

        # Execute the join query
        df_joined = self.spark.sql(join_query)
        return df_joined


# Your PayrollDataProcessor class definition goes here

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PayrollDataProcessorApp").getOrCreate()
    
    # Initialize your PayrollDataProcessor with the Spark session
    processor = PayrollDataProcessor(spark)
    
    # Define your table paths (update these placeholders with your actual paths)
    wkly_planner_path = "path/to/weekly_planner"
    snapshot_path = "path/to/snapshot"
    ref1_path = "path/to/ref1"
    ref2_path = "path/to/ref2"
    
    # Reading and processing tables
    # Assuming the methods are designed to read from Azure Blob and process them
    df_wkly_planner = processor.alpha_read_table(wkly_planner_path)
    df_wkly_planner_processed = processor.process_wkly_planner(df_wkly_planner)
    
    df_snapshot = processor.beta_read_table(snapshot_path)
    df_snapshot_processed = processor.process_snapshot(df_snapshot)
    
    df_ref1 = processor.charlie_read_table(ref1_path)
    df_ref1_processed = processor.process_ref(df_ref1)
    
    df_ref2 = processor.charlie_read_table(ref2_path)
    df_ref2_processed = processor.process_ref2(df_ref2)
    
    # Example of joining snapshot with reference tables
    # Here you should define how df_snapshot_processed, df_ref1_processed, and df_ref2_processed are passed and used
    # Assuming you need to join them, ensure the dataframes are correctly prepared and available for joining
    # For demonstration, the join logic inside join_ss_ref should be implemented or adjusted based on actual requirements
    
    # Make sure the join_ss_ref method is ready to handle the dataframes correctly
    # The example join logic assumes you're ready to perform SQL operations on registered temp views
    # df_joined = processor.join_ss_ref(df_snapshot_processed, df_ref1_processed, df_ref2_processed)

    # Since the actual joining part is not fully detailed in terms of column names and logic,
    # ensure the join_ss_ref method is correctly implemented with the correct SQL JOIN logic.
    
    # Remember to adjust paths, method implementations, and logic to fit your

