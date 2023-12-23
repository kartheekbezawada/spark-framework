from pyspark.sql import SparkSession, Row
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
import traceback
import os

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

    def get_and_log_folder_info(self, container_client, folder_path, log_table_name):
        try:
            folder_info = {
                "folder_path": folder_path,
                "num_files": 0,
                "file_info": []
            }
            for blob in container_client.list_blobs(name_starts_with=folder_path):
                folder_info["num_files"] += 1
                folder_info["file_info"].append({
                    "file_name": blob.name,
                    "file_size_bytes": blob.size,
                    "num_rows": None  # Placeholder for row count
                })

            # Log folder info to SQL MI
            folder_info_df = self.spark.createDataFrame([Row(
                folder_path=folder_info['folder_path'], 
                num_files=folder_info['num_files'],
                timestamp=datetime.now()
            )])
            folder_info_df.write \
                .format("jdbc") \
                .option("url", self.sql_mi_jdbc_url) \
                .option("dbtable", log_table_name) \
                .option("user", self.sql_mi_properties["user"]) \
                .option("password", self.sql_mi_properties["password"]) \
                .mode("append") \
                .save()

            return folder_info
        except Exception as e:
            self.log_error(f"Error getting and logging folder info for {folder_path}: {str(e)}")
            return None

    def list_directories_and_files(self, container_client, container_name):
        directories_and_files = {}
        for blob in container_client.list_blobs(container_name):
            directory = os.path.dirname(blob.name)
            if directory not in directories_and_files:
                directories_and_files[directory] = []
            directories_and_files[directory].append(blob.name)
        return directories_and_files

    def process_folder(self, folder_path, log_table_name):
        folder_info = self.get_and_log_folder_info(self.alpha_blob_service_client, folder_path, log_table_name)
        if folder_info:
            for file_info in folder_info["file_info"]:
                file_path = file_info["file_name"]
                df = self.read_data_from_storage(file_path)
                self.write_to_sql_mi(df, folder_path)
                self.copy_to_beta_storage(df, file_path)
            self.write_tracking_info(folder_path)

    def write_to_sql_mi(self, dataframe, table_name):
        try:
            dataframe.write \
                .format("jdbc") \
                .option("url", self.sql_mi_jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", self.sql_mi_properties["user"]) \
                .option("password", self.sql_mi_properties["password"]) \
                .mode("append") \
                .save()
        except Exception as e:
            self.log_error(f"Error writing to SQL MI: {str(e)}")
    
    def copy_to_beta_storage(self, dataframe, file_path):
        try:
         file_format = "parquet"  # or "csv", etc., based on your requirement
         output_path = f"wasbs://{self.beta_container}@{self.beta_storage_account}.blob.core.windows.net/{file_path}"
         dataframe.write.format(file_format).save(output_path)
        except Exception as e:
            self.log_error(f"Error copying data to Beta Storage: {str(e)}")

    def write_tracking_info(self, folder_path):
        try:
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
        except Exception as e:
            self.log_error(f"Error writing tracking info for {folder_path}: {str(e)}")

    def read_data_from_storage(self, file_path):
        file_extension = os.path.splitext(file_path)[1].lower()
        if file_extension == ".parquet":
            return self.spark.read.parquet(file_path)
        elif file_extension == ".csv":
            return self.spark.read.csv(file_path, header=True, inferSchema=True)
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")

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

    def process_data(self):
        processed_paths = set(self.get_processed_paths())
        alpha_container_client = self.alpha_blob_service_client.get_container_client(self.alpha_container)

        directory_files = self.list_directories_and_files(alpha_container_client, self.alpha_container)

        for directory in directory_files.keys():
            if directory not in processed_paths:
                self.process_folder(directory)
                processed_paths.add(directory)  # Mark this directory as processed

if __name__ == "__main__":
    # Configuration parameters (these should be replaced with your actual parameters)
    key_vault_url = "your_key_vault_url"
    alpha_storage_account = "your_alpha_storage_account"
    alpha_container = "your_alpha_container"
    beta_storage_account = "your_beta_storage_account"
    beta_container = "your_beta_container"
    sql_mi_jdbc_url_key = "your_sql_mi_jdbc_url_key"
    tracking_table = "your_tracking_table"
    log_table_name = "your_log_table_name"
   
    dm = DataMigrator(key_vault_url, alpha_storage_account, alpha_container, beta_storage_account, beta_container, sql_mi_jdbc_url_key, tracking_table)
    dm.process_data()
