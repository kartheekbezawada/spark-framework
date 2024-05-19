from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import deltaTable
import os
import uuid
from pyspark.dbutils import DBUtils
from datetime import datetime

class SalaryJob:
    def __init__(self, spark):
        self.spark = spark
        
        self.instance = '01'
        self.env = os.getenv('environment')
        self.loc = os.getenv('location')
        
        # Get Azure Keyvault Scope
        self.keyvault_scope = f"keyvault-{self.loc}-{self.env}"
        
        # Storage Account Details Alpha Storage
        self.alpha_account_name = f"alphastorage{self.loc}{self.env}"
        self.alpha_container_name = f"alphacontainer{self.loc}{self.env}"
        self.alpha_account_key = DBUtils(self.spark).secrets.get(scope=self.keyvault_scope, key=f"storage-{self.loc}-{self.env}")
        
        # Storage Account Details Bravo Storage
        self.bravo_account_name = f"bravostorage{self.loc}{self.env}"
        self.bravo_container_name = f"bravocontainer{self.loc}{self.env}"
        self.bravo_account_key = DBUtils(self.spark).secrets.get(scope=self.keyvault_scope, key=f"storage-{self.loc}-{self.env}")
        
        self.alpha_storage_url = f"wasbs://{self.alpha_account_name}@{self.alpha_container_name}.blob.core.windows.net"
        self.bravo_storage_url = f"wasbs://{self.bravo_account_name}@{self.bravo_container_name}.blob.core.windows.net"
        
        self.set_storage_config()
        
        # Synapse variables
        self.server_name = f"synapse{self.loc}{self.env}"
        self.database_name = f"synapsedb{self.loc}{self.env}"
        self.user_name = "sqladmin"
        self.password = DBUtils(self.spark).secrets.get(scope=self.keyvault_scope, key=f"synapse-{self.loc}-{self.env}")
        self.url = f"jdbc:sqlserver://{self.server_name}.sql.azuresynapse.net:1433;database={self.database_name};user={self.user_name};password={self.password};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;"
    
        # Log Metadata
        self.job_id = str(uuid.uuid4())
        self.target_system_type = "delta table"
        self.job_status = " "  # if successful then "Success" else "Failed"
        self.load_type = " "
        self.rows_processed = 0  # number of rows written by dataframe in delta table 
        self.job_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.job_end_time = None
        self.job_date = datetime.now().strftime("%Y-%m-%d")
        
    def set_storage_config(self):
        self.spark.conf.set(f"fs.azure.account.key.{self.alpha_account_name}.blob.core.windows.net", self.alpha_account_key)
        self.spark.conf.set(f"fs.azure.account.key.{self.bravo_account_name}.blob.core.windows.net", self.bravo_account_key)
        
    def synapse_connection(self):
        df = self.spark.read \
                .format("com.databricks.spark.sqldw") \
                .option("url", self.url) \
                .option("dbtable", "dbo.salary") \
                .option("user", self.user_name) \
                .option("password", self.password) \
                .load()
        if df.count() > 0:
            print("Connection is successful")
        else:
            print("Connection failed")
    
    # Read from alpha storage
    def read_alpha_storage(self):
        df = self.spark.read.csv(f"{self.alpha_storage_url}/salary.csv", header=True)
        return df
    
    # Write to bravo storage using partition overwrite mode dynamic partition
    def write_delta_table_overwrite(self, df, write_mode, partition_columns, delta_table_path):
        self.load_type = write_mode
        write_mode = "overwrite"
        full_delta_table_path = f"{self.bravo_storage_url}/{delta_table_path}"
        try:
            df.write.format("delta") \
                .mode(write_mode) \
                .partitionBy(partition_columns) \
                .option("partitionOverwriteMode", "dynamic") \
                .save(full_delta_table_path)  
            self.job_status = "Success"
            self.rows_processed = df.count()
        except Exception as e:
            self.job_status = "Failed"
            print(f"Error writing to Delta table: {e}")
        
        self.job_end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        metadata_df = self.log_metadata()
        self.write_metadata_to_synapse(metadata_df)
        
        return df
    
    def write_delta_table_append(self, df, write_mode, partition_columns, delta_table_path):
        self.load_type = write_mode
        write_mode = "append"
        full_delta_table_path = f"{self.bravo_storage_url}/{delta_table_path}"
        try:
            df.write.format("delta") \
                .mode(write_mode) \
                .partitionBy(partition_columns) \
                .option("partitionOverwriteMode", "dynamic") \
                .save(full_delta_table_path)  
            self.job_status = "Success"
            self.rows_processed = df.count()
        except Exception as e:
            self.job_status = "Failed"
            print(f"Error writing to Delta table: {e}")
        
        self.job_end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        metadata_df = self.log_metadata()
        self.write_metadata_to_synapse(metadata_df)
        
        return df
    
    # Log metadata
    def log_metadata(self):
        schema = StructType([
            StructField("JobID", StringType(), False),
            StructField("TargetSystemType", StringType(), False),
            StructField("JobStatus", StringType(), False),
            StructField("RowsProcessed", IntegerType(), False),
            StructField("JobStartTime", StringType(), False),
            StructField("JobEndTime", StringType(), False),
            StructField("JobDate", StringType(), False),
            StructField("LoadType", StringType(), False)
        ])
        
        metadata = [(self.job_id, self.target_system_type, self.job_status, self.rows_processed, self.job_start_time, self.job_end_time, self.job_date, self.load_type)]
        metadata_df = self.spark.createDataFrame([metadata])
        return metadata_df

    # Write metadata to Synapse table
    def write_metadata_to_synapse(self, metadata_df):
        metadata_df.write \
            .format("com.databricks.spark.sqldw") \
            .option("url", self.url) \
            .option("dbtable", "dbo.job_metadata") \
            .option("user", self.user_name) \
            .option("password", self.password) \
            .mode("append") \
            .save()

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Synapse Connection Check").getOrCreate()
    salary_job = SalaryJob(spark)
    salary_job.synapse_connection()
    df = salary_job.read_alpha_storage()
    # Partition columns is a list of columns based on year, month, date
    partition_columns = ["year", "month", "date"]
    delta_table_path = "delta_table_path"
    salary_job.write_delta_table(df, write_mode, partition_columns, delta_table_path)
    spark.stop()
