# True Parallel Processing of Tables

import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.utils import AnalysisException
import datetime
import hashlib 
import os
import random
import dbutils
from azure.storage.blob import BlobServiceClient
from concurrent.futures import ThreadPoolExecutor, as_completed

class DatabricksConnector:
    def __init__(self, spark, key_vault_scope):
        self.spark = spark
        self.key_vault_scope = key_vault_scope
        
        # SQL Server credentials
        self.jdbc_hostname = dbutils.secrets.get(scope=self.key_vault_scope, key="sql-server-hostname")
        self.jdbc_database = dbutils.secrets.get(scope=self.key_vault_scope, key="sql-database-name")
        self.jdbc_username = dbutils.secrets.get(scope=self.key_vault_scope, key="sql-username")
        self.jdbc_password = dbutils.secrets.get(scope=self.key_vault_scope, key="sql-password")
        
        # Azure Data Lake Storage Gen2 credentials for alpha
        self.alpha_account_name = dbutils.secrets.get(scope=self.key_vault_scope, key="blob-storage-account-name")
        self.alpha_account_key = dbutils.secrets.get(scope=self.key_vault_scope, key="blob-storage-account-key")
        self.alpha_container_name = dbutils.secrets.get(scope=self.key_vault_scope, key="blob-container-name")
        self.alpha_storage_url = self._get_blob_storage_url()
        self.alpha_storage_url_https = self.get_blob_storage_url_https()
        self.alpha_storage_config = self._get_blob_storage_config()

    def _get_blob_storage_url(self):
        return f"wasbs://{self.alpha_container_name}@{self.alpha_account_name}.blob.core.windows.net"

    def get_blob_storage_url_https(self):
        return f"https://{self.alpha_account_name}.blob.core.windows.net"

    def _get_blob_storage_config(self):
        return {f"fs.azure.account.key.{self.alpha_account_name}.blob.core.windows.net": self.alpha_account_key}

    def _get_jdbc_url(self):
        return f"jdbc:sqlserver://{self.jdbc_hostname};database={self.jdbc_database}"
    
    def read_from_blob_storage(self, blob_path):
        try:
            df = self.spark.read \
                .format("parquet") \
                .options(**self.alpha_storage_config) \
                .option("recursiveFileLookup", "true") \
                .option("inferSchema", "true") \
                .option("header", "true") \
                .load(blob_path)
            return df
        except Exception as e:
            print(f"Error reading from blob storage: {e}")
            raise e
    
    def get_table_name_from_blob_path(self, blob_path):
        table_name = blob_path.split('/')[-1]
        return table_name
    
    def get_row_count(self, blob_path):
        df = self.read_from_blob_storage(blob_path)
        return df.count()
    
    def write_to_sql_server(self, df, table_name):
        try:
            df.write \
                .format("jdbc") \
                .option("url", self._get_jdbc_url()) \
                .option("dbtable", table_name) \
                .option("user", self.jdbc_username) \
                .option("password", self.jdbc_password) \
                .mode("append") \
                .save()
        except Exception as e:
            print(f"Error writing to SQL Server: {e}")
            raise e
    
    def migration_log_info(self, table_name, blob_path):
        try:
            current_time = datetime.datetime.now()
            row_count = self.get_row_count(blob_path)

            log_df = self.spark.createDataFrame([
                Row(
                    table_name=table_name,
                    row_count=row_count,
                    blob_path=blob_path,
                    timestamp=current_time.strftime("%Y-%m-%d %H:%M:%S")
                )
            ])

            log_df.write \
                .format("jdbc") \
                .option("url", self._get_jdbc_url()) \
                .option("dbtable", "Migration_Log_Table") \
                .option("user", self.jdbc_username) \
                .option("password", self.jdbc_password) \
                .mode("append") \
                .save()
        except Exception as e:
            print(f"Error writing to SQL Server log: {e}")
            raise e

    def get_all_folders(self, container_name):
        try:
            blob_service_client = BlobServiceClient(account_url=self.alpha_storage_url_https, credential=self.alpha_account_key)
            container_client = blob_service_client.get_container_client(container_name)
            blob_list = container_client.list_blobs()

            folders = set()
            for blob in blob_list:
                directory_path = '/'.join(blob.name.split('/')[:-1])
                if directory_path:
                    folders.add(directory_path)

            return list(folders)
        except Exception as e:
            print(f"Error getting all folders in container {container_name}: {e}")
            raise e

    def get_folders_size_in_mb(self, container_name):
        try:
            blob_service_client = BlobServiceClient(account_url=self.alpha_storage_url, credential=self.alpha_account_key)
            container_client = blob_service_client.get_container_client(container_name)
            blob_list = container_client.list_blobs()

            folder_sizes = {}
            for blob in blob_list:
                directory_path = '/'.join(blob.name.split('/')[:-1])
                if directory_path:
                    folder_sizes[directory_path] = folder_sizes.get(directory_path, 0) + blob.size

            for folder in folder_sizes:
                folder_sizes[folder] = folder_sizes[folder] / (1024 * 1024)

            return folder_sizes
        except Exception as e:
            print(f"Error getting sizes of folders in container {container_name}: {e}")
            raise e

    def delete_folder_from_blob_storage(self, folder_path):
        try:
            blob_service_client = BlobServiceClient(account_url=self.alpha_storage_url_https, credential=self.alpha_account_key)
            container_client = blob_service_client.get_container_client(self.alpha_container_name)
            blobs = container_client.list_blobs(name_starts_with=folder_path)
            for blob in blobs:
                blob_client = container_client.get_blob_client(blob)
                blob_client.delete_blob()
            print(f"Deleted folder: {folder_path} from blob storage")
        except Exception as e:
            print(f"Error deleting folder from blob storage: {e}")
            raise e

    def process_table(self, table_name):
        blob_path = f"{self.alpha_storage_url}/{table_name}"
        df = self.read_from_blob_storage(blob_path)
        if df is not None and not df.rdd.isEmpty():
            self.write_to_sql_server(df, table_name=table_name)
            self.migration_log_info(table_name, blob_path)
            self.delete_folder_from_blob_storage(table_name)
        else:
            print(f"No data found in blob path: {blob_path}")

    def process_all_tables(self, table_names, max_workers=5):
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_table = {executor.submit(self.process_table, table_name): table_name for table_name in table_names}
            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    future.result()
                    print(f"Data migration completed for table: {table_name}")
                except Exception as e:
                    print(f"Error processing table {table_name}: {e}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Migration").getOrCreate()
    key_vault_scope = "key_vault_scope_migration"
    databricks_connector = DatabricksConnector(spark, key_vault_scope)
    container_name = "data"

    # Define your strategy to determine which tables to process
    # Example: Process all tables
    table_names = databricks_connector.get_all_folders(container_name)

    # Process tables in parallel
    databricks_connector.process_all_tables(table_names, max_workers=5)
