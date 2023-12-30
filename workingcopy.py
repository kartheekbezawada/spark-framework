import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.utils import AnalysisException
import datetime
import hashlib 
import os
import random
import dbutils

class DatabricksConnector:
    def __init__(self, spark, key_vault_scope):
        self.spark = spark
        self.key_vault_scope = "key_vault_scope_migration"
        
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
    
    #read from blob storage where blob path is actual folder and recursive is true
    def read_from_blob_storage(self, blob_path):
        try:
            df = self.spark.read \
                .format("parquet") \
                .options(**self.alpha_storage_config) \
                .option("recursiveFileLookup", "true") \
                .option("inferSchema", "true") \
                .option("header", "true") \
                .load(blob_path)
        except Exception as e:
            print(f"Error reading from blob storage: {e}")
            raise e
        return df
    
    def get_table_name_from_blob_path(self,blob_path):
        table_name = blob_path.split('/')[-1]  # Split the path and take the last part
        return table_name
    
    def get_row_count(self, blob_path):
        df = self.read_from_blob_storage(blob_path)
        return df.count()
    
    # write to sql server by creating table, table not present in sql server
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
    
    # Write table name,row count, blob path, current time to sql server
    def migration_log_info(self, table_name, blob_path):
        try:
            current_time = datetime.datetime.now()
            row_count = self.get_row_count(blob_path)

        # Ensure that all fields in Row are compatible with SQL Server
            log_df = self.spark.createDataFrame([
                Row(
                table_name=table_name,
                row_count=row_count,
                timestamp=current_time.strftime("%Y-%m-%d %H:%M:%S")  # Format timestamp
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

            # Using a set to store unique folder names
            folders = set()
            for blob in blob_list:
                # Extract directory names
                directory_path = '/'.join(blob.name.split('/')[:-1])
                if directory_path:  # If it's not an empty string, add to the set
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

            # Dictionary to store folder sizes
            folder_sizes = {}
            for blob in blob_list:
                # Extract directory names
                directory_path = '/'.join(blob.name.split('/')[:-1])
                if directory_path:
                    # Accumulate sizes for each folder
                    folder_sizes[directory_path] = folder_sizes.get(directory_path, 0) + blob.size

            # Convert sizes to MB
            for folder in folder_sizes:
                folder_sizes[folder] = folder_sizes[folder] / (1024 * 1024)  # Convert from bytes to MB

            return folder_sizes
        except Exception as e:
            print(f"Error getting sizes of folders in container {container_name}: {e}")
            raise e
    
    def get_top_n_folders_by_size(self, container_name, num_folders):
        try:
            # Get folder sizes in MB
            folder_sizes_in_mb = self.get_folders_size_in_mb(container_name)

            # Sort the folders by size and get the top 'n'
            top_folders = sorted(folder_sizes_in_mb.items(), key=lambda x: x[1], reverse=True)[:num_folders]
            top_folders_key = [items[0] for items in top_folders]
            return top_folders_key
        except Exception as e:
            print(f"Error getting top {n} folders in container {container_name}: {e}")
            raise e
    
    def delete_directory_from_blob_storage(self, directory_path):
        try:
            # Initialize the Data Lake Service Client
            service_client = DataLakeServiceClient(account_url=self.alpha_storage_url, credential=self.alpha_account_key)

            # Get a client for the file system and then for the directory
            file_system_client = service_client.get_file_system_client(file_system=self.alpha_container_name)
            directory_client = file_system_client.get_directory_client(directory_path)

            # Delete the directory
            directory_client.delete_directory()

            print(f"Deleted directory: {directory_path} from Azure Data Lake Storage Gen2")
        except Exception as e:
            print(f"Error deleting directory from Azure Data Lake Storage Gen2: {e}")
            raise e
    
    def process_table(self, table_name):
        blob_path = f"{self.alpha_storage_url}/{table_name}"
        directory_path = table_name  # Assuming table_name corresponds to the directory name in ADLS Gen2

        try:
            df = self.read_from_blob_storage(blob_path)
            if df is not None and not df.rdd.isEmpty():
                self.write_to_sql_server(df, table_name=table_name)
                self.migration_log_info(table_name, blob_path)
                
                # Delete the directory after successful migration
                self.delete_directory_from_blob_storage(directory_path)
                print(f"Successfully processed and deleted directory: {directory_path}")
            else:
                print(f"No data found in blob path: {blob_path}")
        except Exception as e:
            print(f"Error processing table {table_name}: {e}")
    
    # Process all tables that come from get_all_folders
    def process_all_tables(self, table_names):
        for table_name in table_names:
            self.process_table(table_name)
            print(f"Data migrated for table: {table_name}")
            
   
    #This code will loop back agian and again until the data is migrated from blob storage
    def batch_process_all_tables(self, container_name, batch_size):
    # Get the sorted list of folders by size
        sorted_folders = self.get_top_n_folders_by_size(container_name, batch_size)
    # Process folders in batches
        for i in range(0, len(sorted_folders), batch_size):
            batch_folders = sorted_folders[i:i + batch_size]
            print(f"Processing batch: {batch_folders}")
            self.process_all_tables(batch_folders)
            print(f"Data migration completed for batch: {batch_folders}")


if __name__ == "__main__":
    # Define the flags
    process_all_tables_flag = True  # Set this to True to process all tables at once
    batch_process_flag = False  
    process_given_tables = False 

    # Initialize Spark session and Databricks connector
    spark = SparkSession.builder.appName("Migration").getOrCreate()
    key_vault_scope = "key_vault_scope_migration"
    databricks_connector = DatabricksConnector(spark, key_vault_scope)
    container_name = "data"
    batch_size = 2  # Define the batch size for batch processing

    # Process all tables if the flag is set to True
    if process_all_tables_flag:
        table_name = databricks_connector.get_all_folders(container_name)
        databricks_connector.process_all_tables(table_name)
        print("Completed processing all tables.")

    # Else, process tables in batches
    elif batch_process_flag:
        databricks_connector.batch_process_all_tables(container_name, batch_size)
        print(f"Completed batch processing of tables in container '{container_name}'.")
        
    elif process_given_tables:
        table_names = ["data/2019/01/01", "data/2019/01/02"]
        databricks_connector.process_all_tables(table_names)
        print(f"Completed processing of tables: {table_names}")



    