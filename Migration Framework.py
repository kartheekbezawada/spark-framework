import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.utils import AnalysisException
from azure.storage.blob import BlobServiceClient
import datetime
import hashlib 
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
        
    # Write to sql server by using SQL - Spark Connector
    def write_to_sql_server_2(self, df, table_name):
        try:
            df.write \
                .format("com.microsoft.sqlserver.jdbc.spark") \
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
    def migration_log_info(self, table_name, blob_path, row_count_validated):
        try:
            current_time = datetime.datetime.now()
            row_count = self.get_row_count(blob_path)
            validation_status = "Pass" if row_count_validated else "Fail"
            log_df = self.spark.createDataFrame([
                Row(
                    table_name=table_name,
                    row_count=row_count,
                    validation_status=validation_status,
                    timestamp=current_time.strftime("%Y-%m-%d %H:%M:%S"))])
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


    def destination_row_count(self, table_name):
        """ Count the number of rows in a destination table """
        dest_df = self.spark.read \
            .format("jdbc") \
            .option("url", self._get_jdbc_url()) \
            .option("dbtable", table_name) \
            .option("user", self.jdbc_username) \
            .option("password", self.jdbc_password) \
            .load()
        return dest_df.count()

    def validate_data(self, source_df, dest_table_name):
        """ Validate the data integrity between source and destination by comparing row counts """
        source_row_count = source_df.count()
        dest_row_count = self.destination_row_count(dest_table_name)
        if source_row_count == dest_row_count:
            print(f"Validation successful for table {dest_table_name}")
            return True
        else:
         print(f"Validation failed for table {dest_table_name}. Source count: {source_row_count}, Destination count: {dest_row_count}")
        return False

    
    # Ignore tables that have already been migrated
    def ignore_migrated_tables(self, table_names):
        try:
            migrated_tables = self.spark.read \
                .format("jdbc") \
                .option("url", self._get_jdbc_url()) \
                .option("dbtable", "Migration_Log_Table") \
                .option("user", self.jdbc_username) \
                .option("password", self.jdbc_password) \
                .load() \
                .select("Migrated_Table_Name") \
                .distinct() \
                .collect()
            migrated_tables = [row.Migrated_Table_Name for row in migrated_tables]
            return [table_name for table_name in table_names if table_name not in migrated_tables]
        except AnalysisException as e:
            print(f"Error ignoring migrated tables: {e}")
            return table_names

    
    # Process a single table
    def process_table(self, table_name):
        blob_path = f"{self.alpha_storage_url}/{table_name}"
        source_df = self.read_from_blob_storage(blob_path)
        
        if source_df is not None and not source_df.rdd.isEmpty():
            self.write_to_sql_server(source_df, table_name=table_name)
            validation_result = self.validate_data(source_df, table_name)
            self.migration_log_info(table_name, blob_path, validation_result)         
        else:
            print(f"No data found in blob path: {blob_path}")
    
    # Process all tables in the list of table names except those that have already been migrated
    def process_all_tables(self, table_names):
        table_names = self.ignore_migrated_tables(table_names)
        for table_name in table_names:
            self.process_table(table_name)
            print(f"Data migrated for table: {table_name}")
            
    # Batch process all tables in the list of table names except those that have already been migrated
    def batch_process_all_tables(self, container_name, batch_size):
        all_folders = self.get_all_folders(container_name)
        ignore_migrated_folders = self.ignore_migrated_tables(all_folders)

        # Get folder sizes and sort the non-migrated folders
        folder_sizes = self.get_folders_size_in_mb(container_name)
        sorted_folders = sorted(ignore_migrated_folders, key=lambda x: folder_sizes.get(x, 0), reverse=True)

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



    