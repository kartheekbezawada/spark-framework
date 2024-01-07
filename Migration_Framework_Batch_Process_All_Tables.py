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
    
    def get_table_name_from_blob_path(self,blob_path):
        table_name = blob_path.split('/')[-1]  # Split the path and take the last part
        return table_name
    
    def get_row_count(self, blob_path):
        df = self.read_from_blob_storage(blob_path)
        return df.count()
    
    def start_metrics_collection(self):
        return {"start_time": datetime.datetime.now(), "end_time": None, "status": "Success", "error_message": None}

    def update_metrics_on_completion(self, metrics):
        metrics["end_time"] = datetime.datetime.now()

    def update_metrics_on_failure(self, metrics, error_message):
        metrics["end_time"] = datetime.datetime.now()
        metrics["status"] = "Fail"
        metrics["error_message"] = str(error_message)
    
    
    #read from blob storage where blob path is actual folder and recursive is true
    def read_from_blob_storage(self, blob_path): 
        read_metrics = self.start_metrics_collection()
        try:
            df = self.spark.read \
                .format("parquet") \
                .options(**self.alpha_storage_config) \
                .option("recursiveFileLookup", "true") \
                .option("inferSchema", "true") \
                .option("header", "true") \
                .load(blob_path)
            self.update_metrics_on_completion(read_metrics)
        except Exception as e:
            self.update_metrics_on_failure(read_metrics, e)
            print(f"Error reading from blob storage: {e}")
        finally:
            self.log_read_metrics(table_name, blob_path,read_metrics)
            return df, read_metrics
    

    
    # write to sql server by creating table, table not present in sql server
    def write_to_sql_server(self, df, table_name):
        write_metrics = self.start_metrics_collection()
        try:
            write_metrics["Write_Start_Time"] = datetime.datetime.now()
            df.write \
                .format("jdbc") \
                .option("url", self._get_jdbc_url()) \
                .option("dbtable", table_name) \
                .option("user", self.jdbc_username) \
                .option("password", self.jdbc_password) \
                .mode("append") \
                .save()
            self.update_metrics_on_completion(write_metrics)
        except Exception as e:
            self.update_metrics_on_failure(write_metrics, e)
            print(f"Error writing to SQL Server: {e}")
        finally:
            self.log_write_metrics(table_name, blob_path, write_metrics)
            return write_metrics
        
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
    def migration_log_info(self, table_name, blob_path, row_count_validated, read_metrics, write_metrics):
        try:
            current_time = datetime.datetime.now()
            row_count = self.get_row_count(blob_path)
            validation_status = "Pass" if row_count_validated else "Fail"
            log_df = self.spark.createDataFrame([
                Row(
                    table_name=table_name,
                    blob_path=blob_path,
                    row_count=row_count,
                    validation_status=validation_status,
                    timestamp=current_time.strftime("%Y-%m-%d %H:%M:%S"),
                    **{f"Read_{k}": v for k, v in read_metrics.items()},
                    **{f"Write_{k}": v for k, v in write_metrics.items()} 
                    )])
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
        source_df, read_metrics = self.read_from_blob_storage(blob_path,table_name)
        
        if source_df is not None and not source_df.rdd.isEmpty():
            write_metrics = self.write_to_sql_server(source_df, table_name)
            validation_result = self.validate_data(source_df, table_name)
            self.migration_log_info(table_name, blob_path, validation_result, read_metrics, write_metrics)       
        else:
            print(f"No data found in blob path: {blob_path}")
            
    def process_batch_parallel(self, batch_folders):
        # Method to process a batch of tables
        try:
            for table_name in batch_folders:
                self.process_table(table_name)
        except Exception as e:
            print(f"Error processing batch {batch_folders}: {e}")
            
    # Batch process all tables in the list of table names except those that have already been migrated
    def batch_process_all_tables_parallel(self, container_name, batch_size):
        # Method to process all tables in parallel batches
        all_folders = self.get_all_folders(container_name)
        ignore_migrated_folders = self.ignore_migrated_tables(all_folders)

        # Split into batches and process in parallel
        batches = [ignore_migrated_folders[i:i + batch_size] for i in range(0, len(ignore_migrated_folders), batch_size)]
        rdd = self.spark.sparkContext.parallelize(batches)
        rdd.foreach(self.process_batch_parallel)
        print(f"Completed parallel batch processing of tables in container '{container_name}'.")
        

if __name__ == "__main__":
    # Define your Spark session and DatabricksConnector
    spark = SparkSession.builder.appName("DataMigration").getOrCreate()
    key_vault_scope = "your_key_vault_scope"
    databricks_connector = DatabricksConnector(spark, key_vault_scope)
    
    # Set the container name and batch size
    container_name = "your_container_name"
    batch_size = 5  # Adjust the batch size based on your scenario

    # Start the parallel batch processing of tables
    databricks_connector.batch_process_all_tables_parallel(container_name, batch_size)


    