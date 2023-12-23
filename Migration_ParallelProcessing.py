from pyspark.sql import SparkSession, Row, DataFrame
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
import os
from functools import reduce
import time

class DataMigrator:
    def __init__(self, key_vault_url, alpha_storage_account, alpha_container, beta_storage_account, beta_container, sql_mi_jdbc_url_key, tracking_table):
        self.spark = SparkSession.builder.appName("DataMigration").getOrCreate()
        self.key_vault_url = key_vault_url
        self.alpha_storage_account = alpha_storage_account
        self.alpha_container = alpha_container
        self.beta_storage_account = beta_storage_account
        self.beta_container = beta_container
        self.sql_mi_jdbc_url_key = sql_mi_jdbc_url_key
        self.tracking_table = tracking_table
        self.load_secrets()
        self.connect_to_blob_storage()
        self.connect_to_sql_server()

    def load_secrets(self):
        credential = DefaultAzureCredential()
        self.client = SecretClient(vault_url=self.key_vault_url, credential=credential)
        self.sql_mi_properties = {
            "user": self.client.get_secret("sql-mi-username").value,
            "password": self.client.get_secret("sql-mi-password").value
        }
        self.sql_mi_jdbc_url = self.client.get_secret(self.sql_mi_jdbc_url_key).value

    def connect_to_blob_storage(self):
        self.alpha_blob_service_client = BlobServiceClient(account_url=f"https://{self.alpha_storage_account}.blob.core.windows.net", credential=DefaultAzureCredential())
        self.beta_blob_service_client = BlobServiceClient(account_url=f"https://{self.beta_storage_account}.blob.core.windows.net", credential=DefaultAzureCredential())

    def connect_to_sql_server(self):
        # Placeholder for SQL Server connection logic
        pass

    def log_error(self, error_message, table_name="migration_error_log"):
        current_time = datetime.now()
        error_df = self.spark.createDataFrame([Row(timestamp=current_time, error=error_message)])
        error_df.write \
            .format("jdbc") \
            .option("url", self.sql_mi_jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", self.sql_mi_properties["user"]) \
            .option("password", self.sql_mi_properties["password"]) \
            .mode("append") \
            .save()
    
    def count_rows_in_parquet_file(self, file_path):
        try:
            df = self.spark.read.parquet(file_path)
            return df.count()
        except Exception as e:
            self.log_error(f"Error counting rows in Parquet file {file_path}: {str(e)}")
            return 0  # Return 0 if unable to count rows

    def get_and_log_folder_info(self, container_client, folder_path, log_table_name):
        try:
            folder_info = {
                "folder_path": folder_path,
                "num_files": 0,
                "file_info": [],
                "total_rows": 0,
                "total_size_gb": 0.0  # Total size of all files in GB
            }
            for blob in container_client.list_blobs(name_starts_with=folder_path):
                file_path = f"wasbs://{self.alpha_container}@{self.alpha_storage_account}.blob.core.windows.net/{blob.name}"
                file_rows = self.count_rows_in_parquet_file(file_path)
                file_size_gb = blob.size / (1024 * 1024 * 1024)  # Convert from bytes to GB

                folder_info["num_files"] += 1
                folder_info["total_rows"] += file_rows
                folder_info["total_size_gb"] += file_size_gb  # Accumulate total size
                folder_info["file_info"].append({
                "file_name": blob.name,
                "file_size_bytes": blob.size,
                "file_size_gb": file_size_gb,
                "num_rows": file_rows
            })

            folder_info_df = self.spark.createDataFrame([Row(
                folder_path=folder_info['folder_path'],
                num_files=folder_info['num_files'],
                total_rows=folder_info['total_rows'],
                total_size_gb=folder_info['total_size_gb'],
                timestamp=datetime.now()
            )])
            folder_info_df.write \
                .format("jdbc") \
                .option("url", self.sql_mi_jdbc_url) \
                .option("dbtable", log_table_name) \
                .option("user", self.sql_mi_properties["user"]) \
                .option("password", self.sql_mi_properties["password"]) \
                .mode("append") \
            .   save()

            return folder_info
        except Exception as e:
            self.log_error(f"Error getting and logging folder info for {folder_path}: {str(e)}")
            return None
    


    def read_data_from_storage_parallel(self, file_paths):
        start_time = time.time()
        end_time = time.time()
        duration = end_time - start_time
        dataframes = [self.spark.read.parquet.load(file_path) for file_path in file_paths]
        combined_df = reduce(DataFrame.unionAll, dataframes)
        self.log_performance_metrics("read", duration, additional_info=f"Read {len(file_paths)} files")
        return combined_df

    
    def write_to_sql_mi(self, dataframe, table_name):
        start_time = time.time()
        end_time = time.time()
        duration = end_time - start_time
        dataframe.write \
            .format("jdbc") \
            .option("url", self.sql_mi_jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", self.sql_mi_properties["user"]) \
            .option("password", self.sql_mi_properties["password"]) \
            .mode("append") \
            .save()
    self.log_performance_metrics("write_sql_mi", duration, additional_info=f"Write to {table_name}")

    
    def copy_to_beta_storage(self, dataframe, file_path):
        start_time = time.time()
        end_time = time.time()
        duration = end_time - start_time
        output_path = f"wasbs://{self.beta_container}@{self.beta_storage_account}.blob.core.windows.net/{file_path}"
        dataframe.write.format("your_format").save(output_path)
        self.log_performance_metrics("write_beta_storage", duration, additional_info=f"Write to Beta Storage: {file_path}")

    
    def write_tracking_info(self, folder_path):
        current_time = datetime.now()
        tracking_df = self.spark.createDataFrame([Row(timestamp=current_time, folder_path=folder_path)])
        tracking_df.write \
            .format("jdbc") \
            .option("url", self.sql_mi_jdbc_url) \
            .option("dbtable", self.tracking_table) \
            .option("user", self.sql_mi_properties["user"]) \
            .option("password", self.sql_mi_properties["password"]) \
            .mode("append") \
            .save()

    def list_directories_and_files(self, container_client, container_name):
        directories_and_files = {}
        for blob in container_client.list_blobs(container_name):
            directory = os.path.dirname(blob.name)
            if directory not in directories_and_files:
                directories_and_files[directory] = []
            directories_and_files[directory].append(blob.name)
        return directories_and_files

    def get_processed_paths(self):
        try:
            return self.spark.read \
                .format("jdbc") \
                .option("url", self.sql_mi_jdbc_url) \
                .option("dbtable", self.tracking_table) \
                .option("user", self.sql_mi_properties["user"]) \
                .option("password", self.sql_mi_properties["password"]) \
                .load() \
                .select("table_path") \
                .rdd \
                .map(lambda row: row.table_path) \
                .collect()
        except Exception as e:
            self.log_error(f"Error reading from tracking table: {str(e)}")
            return []
        
    def process_folder(self, folder_path):
        folder_info = self.get_and_log_folder_info(self.alpha_blob_service_client, folder_path, self.tracking_table)
        if folder_info:
            file_paths = [file_info["file_name"] for file_info in folder_info["file_info"]]
            df = self.read_data_from_storage_parallel(file_paths)
            self.write_to_sql_mi(df, folder_path)
            self.copy_to_beta_storage(df, folder_path)
            self.write_tracking_info(folder_path)
        
    def process_data(self):
        processed_paths = set(self.get_processed_paths())
        alpha_container_client = self.alpha_blob_service_client.get_container_client(self.alpha_container)
        directory_files = self.list_directories_and_files(alpha_container_client, self.alpha_container)

        for directory in directory_files.keys():
            if directory not in processed_paths:
                self.process_folder(directory)
                
def log_performance_metrics(self, operation, duration, additional_info="", table_name="performance_metrics_log"):
    try:
        current_time = datetime.now()
        metrics_df = self.spark.createDataFrame([
            Row(timestamp=current_time, operation=operation, duration=duration, additional_info=additional_info)
        ])
        metrics_df.write \
            .format("jdbc") \
            .option("url", self.sql_mi_jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", self.sql_mi_properties["user"]) \
            .option("password", self.sql_mi_properties["password"]) \
            .mode("append") \
            .save()
    except Exception as e:
        self.log_error(f"Error logging performance metrics: {str(e)}")
  

# Main execution
if __name__ == "__main__":
    # Set up and execute DataMigrator
    # Replace these parameters with your actual configuration details
    key_vault_url = "your_key_vault_url"
    alpha_storage_account = "your_alpha_storage_account"
    alpha_container = "your_alpha_container"
    beta_storage_account = "your_beta_storage_account"
    beta_container = "your_beta_container"
    sql_mi_jdbc_url_key = "your_sql_mi_jdbc_url_key"
    tracking_table = "your_tracking_table"

    dm = DataMigrator(key_vault_url, alpha_storage_account, alpha_container, beta_storage_account, beta_container, sql_mi_jdbc_url_key, tracking_table)
    dm.process_data()

