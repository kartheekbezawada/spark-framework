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
    
    def process_cd1(self, df):
        df = df.drop("md_process_id","md_source_ts","md_created_ts","md_source_path")
        return self.prefix_columns(df, "cd1")
    
    def process_scan(self, df):
        df = df.select("store_nbr", "scan_dept_nbr", "retail_price", "other_income_ind","visit_nbr", "visit_date")
        return self.prefix_columns(df, "scan")
    
    def process_visit(self, df):
        df = df.select( "visit_nbr","store_nbr", "visit_date", "register_nbr")
        return self.prefix_columns(df, "visit")
    
    def union_for_processing(self, fsd_df, cd_df, cd1_df):
        # Register DataFrames as temp views
        fsd_df.createOrReplaceTempView("fsd")
        cd_df.createOrReplaceTempView("cd")
        cd1_df.createOrReplaceTempView("cd1")  # Registering cd1_df as a temporary view
    
        # Adjusted SQL query to incorporate 'cd1' in the EXISTS condition
        sql_query = """
        SELECT
            DATE_FORMAT(CURRENT_DATE(), 'dd/MM/yyyy') AS VAW_wtd_date,
            fsd.store_nbr,
            fsd.dept_nbr,
            fsd.summary_date,
            cd.wm_week,
            SUM(fsd.sales_retail_amt) AS total_sales
        FROM
            fsd
        JOIN
            cd ON fsd.summary_date = DATE_FORMAT(cd.calendar_date, 'dd/MM/yyyy')
        WHERE
            EXISTS (
                SELECT 1
                FROM cd1
                WHERE
                    cd1.wm_week = cd.wm_week
                    AND cd1.calendar_year = cd.calendar_year
                    AND cd1.calendar_date = DATE_FORMAT(CURRENT_DATE()-1, 'dd/MM/yyyy')
            )
        GROUP BY
            fsd.store_nbr, fsd.dept_nbr, cd.wm_week
        """
    
        # Execute SQL query
        result_df = self.spark.sql(sql_query)
    
        return result_df
    
    
    def union_two_processing(self, scan_df, visit_df, calendar_df):
        # Filter calendar_df for the current date and extract distinct weeks and years
        current_week_and_year = calendar_df.filter(
            to_date(col('cd_calendar_date'), 'dd/MM/yyyy') == current_date()
        ).select('cd_wm_week', 'cd_calendar_year').distinct()

        # Assuming scan_df, visit_df, and calendar_df have been prepared with the necessary prefixes

        # Join scan_df with visit_df, and then with calendar_df
        # Specify column selection to avoid ambiguity
        joined_df = scan_df.join(
            visit_df,
            (scan_df["scan_visit_nbr"] == visit_df["visit_visit_nbr"]) &
            (scan_df["scan_store_nbr"] == visit_df["visit_store_nbr"]) &
            (to_date(scan_df["scan_visit_date"], 'dd/MM/yyyy') == to_date(visit_df["visit_visit_date"], 'dd/MM/yyyy')) &
            (visit_df["visit_register_nbr"] == 80) &
            scan_df["scan_other_income_ind"].isNull(),
            "inner"
        )

        joined_df = joined_df.join(
            calendar_df,
            to_date(joined_df["scan_visit_date"], 'dd/MM/yyyy') == calendar_df["cd_calendar_date"],
            "inner"
        )

        # To resolve ambiguity, explicitly select the columns to include from each DataFrame before the join
        # This ensures "cd_wm_week" is clearly defined from the correct DataFrame
        final_df = joined_df.alias('a').join(
            current_week_and_year.alias('b'),
            (col('a.cd_wm_week') == col('b.cd_wm_week')) &
            (col('a.cd_calendar_year') == col('b.cd_calendar_year')),
            "inner"
        )

        # Adjust the aggregation to reference the disambiguated column names directly
        result_df = final_df.groupBy("a.scan_store_nbr", "a.cd_wm_week").agg(
            lit(date_format(current_date(), 'dd/MM/yyyy')).alias('VAW_wtd_date'),
            lit(668).alias('dept_nbr'),
            Fsum("a.scan_retail").alias("total_sales")
        ).select("VAW_wtd_date", "a.scan_store_nbr", "dept_nbr", "a.cd_wm_week", "total_sales")

        return result_df
    
    def union_three_processing(self, union_df, union_two_df):
        # Union the two DataFrames
        result_df = union_df.union(union_two_df)
        return result_df
    
    # Write union_three_df to a Delta table
    def write_union_three_df(self, df, delta_table_path):
        full_delta_table_path = f"{self.alpha_storage_url}/{delta_table_path}"
        df.write.format("delta").mode("overwrite").save(full_delta_table_path)
        return df
    
    
if __name__ == "__main__":
    spark = SparkSession.builder.appName("PayrollDataProcessorApp").getOrCreate()
    processor = PayrollDataProcessor(spark)

    fsd_path = "path/to/fsd"
    cd_path = "path/to/cd"
    cd1_path = "path/to/cd1"
    scan_path = "path/to/scan"
    visit_path = "path/to/visit"
    
    fsd_df = processor.alpha_read_table(fsd_path)
    cd_df = processor.beta_read_table(cd_path)
    cd1_df = processor.beta_read_table(cd1_path)
    scan_df = processor.charlie_read_table(scan_path)
    visit_df = processor.charlie_read_table(visit_path)
    
    union_df = processor.union_for_processing(fsd_df, cd_df, cd1_df)
    union_two_df = processor.union_two_processing(scan_df, visit_df, calendar_df)
    unuin_three_df = processor.union_three_processing(union_df, union_two_df)
    
    # Write the result to a Delta table
    delta_table_path = "path/to/your/delta/table"
    processor.write_union_three_df(unuin_three_df, delta_table_path)
    
    
    def union_two_processing(self, scan_df, visit_df, calendar_df):
    # Filter calendar_df for the day before the current date and extract distinct weeks and years
    current_week_and_year = calendar_df.filter(
        F.col('cd_calendar_date') == F.date_sub(F.current_date(), 1)
    ).select('cd_wm_week', 'cd_calendar_year').distinct()

    # Join scan_df with visit_df, and then with calendar_df
    joined_df = scan_df.join(
        visit_df,
        (scan_df["scan_visit_nbr"] == visit_df["visit_visit_nbr"]) &
        (scan_df["scan_store_nbr"] == visit_df["visit_store_nbr"]) &
        (F.to_date(scan_df["scan_visit_date"], 'dd/MM/yyyy') == F.to_date(visit_df["visit_visit_date"], 'dd/MM/yyyy')) &
        (visit_df["visit_register_nbr"] == 80) &
        scan_df["scan_other_income_ind"].isNull(),
        "inner"
    )

    joined_df = joined_df.join(
        calendar_df,
        F.to_date(joined_df["scan_visit_date"], 'dd/MM/yyyy') == F.to_date(calendar_df["cd_calendar_date"], 'dd/MM/yyyy'),
        "inner"
    )

    # Adjust the join to include only records for the day before the current date
    final_df = joined_df.alias('a').join(
        current_week_and_year.alias('b'),
        (F.col('a.cd_wm_week') == F.col('b.cd_wm_week')) &
        (F.col('a.cd_calendar_year') == F.col('b.cd_calendar_year')),
        "inner"
    )

    # Aggregate and select columns
    result_df = final_df.groupBy("a.scan_store_nbr", "a.cd_wm_week").agg(
        F.lit(F.date_format(F.current_date(), 'dd/MM/yyyy')).alias('VAW_wtd_date'),
        F.lit(668).alias('dept_nbr'),  # Assuming dept_nbr is a constant
        F.sum("a.scan_retail").alias("total_sales")
    ).select("VAW_wtd_date", "a.scan_store_nbr", "dept_nbr", "a.cd_wm_week", "total_sales")

    return result_df

    
   