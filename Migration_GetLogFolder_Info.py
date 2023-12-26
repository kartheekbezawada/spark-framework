from pyspark.sql import SparkSession, Row
import os
import dbutils
from azure.storage.blob import BlobServiceClient, ContainerClient


class BlobFolderInfoLogger:
    def __init__(self, spark, key_vault_scope):
        # Use the existing Spark session
        spark = SparkSession.builder.appName("BlobFolderInfoLogger").getOrCreate()
        self.key_vault_scope = "key_vault_scope"
        self.jdbc_hostname = dbutils.secrets.get(scope="key_vault_scope", key="sql-server-hostname")
        self.jdbc_database = dbutils.secrets.get(scope="key_vault_scope", key="sql-database-name")
        self.jdbc_username = dbutils.secrets.get(scope="key_vault_scope", key="sql-username")
        self.jdbc_password = dbutils.secrets.get(scope="key_vault_scope", key="sql-password")
        self.blob_account_name = dbutils.secrets.get(scope="key_vault_scope", key="blob-storage-account-name")
        self.blob_account_key = dbutils.secrets.get(scope="key_vault_scope", key="blob-storage-account-key")
        self.blob_container_name = dbutils.secrets.get(scope="key_vault_scope", key="blob-container-name")
        self.blob_service_client = BlobServiceClient(account_url=f"https://{self.blob_account_name}.blob.core.windows.net", 
                                                     credential=self.blob_account_key)

    def _get_jdbc_url(self):
        return f"jdbc:sqlserver://{self.jdbc_hostname};database={self.jdbc_database}"

    def get_row_count(self, file_path):
        df = self.spark.read.parquet(file_path)
        return df.count()
    
    def _get_file_size_mb(self, item):
        return item.size / (1024 * 1024)

    def _get_file_name(self, file_path):
        return os.path.basename(file_path)

    def get_and_log_folder_info(self, container_client, folder_path, log_table_name):
        container_client = self.blob_service_client.get_container_client(self.blob_container_name)
        try:
            folder_info = {
                "folder_path": folder_path,
                "num_files": 0,
                "file_info": [],
                "total_rows": 0,
                "total_size_gb": 0.0  # Total size of all files in GB
            }
            for blob in container_client.list_blobs(name_starts_with=folder_path):
                file_path = f"wasbs://{self.blob_container_name}@{self.blob_account_name}.blob.core.windows.net/{blob.name}"
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
                .save()

            return folder_info
        except Exception as e:
            self.log_error(f"Error getting and logging folder info for {folder_path}: {str(e)}")
            return None

    # Implement log_error method to handle and log errors
    def log_error(self, message):
        # Placeholder for error logging implementation
        print(message)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("BlobFolderInfoLogger").getOrCreate()
    key_vault_scope = "your_key_vault_scope"  # Replace with your actual key vault scope

    logger = BlobFolderInfoLogger(spark, key_vault_scope)
    folder_path = "your_folder_path"  # Replace with the actual folder path
    log_table_name = "your_log_table_name"  # Replace with the actual log table name

    folder_info = logger.get_and_log_folder_info(folder_path, log_table_name)
    print("Folder Information:", folder_info)

    spark.stop()