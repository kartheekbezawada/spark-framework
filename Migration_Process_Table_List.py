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
        self.jdbc_hostname = dbutils.secrets.get(scope=self.key_vault_scope, key="sql-server-hostname")
        self.jdbc_database = dbutils.secrets.get(scope=self.key_vault_scope, key="sql-database-name")
        self.jdbc_username = dbutils.secrets.get(scope=self.key_vault_scope, key="sql-username")
        self.jdbc_password = dbutils.secrets.get(scope=self.key_vault_scope, key="sql-password")
        self.blob_account_name = dbutils.secrets.get(scope=self.key_vault_scope, key="blob-storage-account-name")
        self.blob_account_key = dbutils.secrets.get(scope=self.key_vault_scope, key="blob-storage-account-key")
        self.blob_container_name = dbutils.secrets.get(scope=self.key_vault_scope, key="blob-container-name")
        self.blob_storage_url = self._get_blob_storage_url()
        self.blob_storage_config = self._get_blob_storage_config()

    def _get_blob_storage_url(self):
        return f"wasbs://{self.blob_container_name}@{self.blob_account_name}.blob.core.windows.net"

    def _get_blob_storage_config(self):
        return {f"fs.azure.account.key.{self.blob_account_name}.blob.core.windows.net": self.blob_account_key}

    def _get_jdbc_url(self):
        return f"jdbc:sqlserver://{self.jdbc_hostname};database={self.jdbc_database}"
    
    #read from blob storage where blob path is actual folder and recursive is true
    def read_from_blob_storage(self, blob_path):
        try:
            df = self.spark.read \
                .format("parquet") \
                .options(**self.blob_storage_config) \
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
                blob_path=blob_path,
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
    
    def process_table(self, table_name):
        blob_path = f"{self.blob_storage_url}/{table_name}"
        df = self.read_from_blob_storage(blob_path)  # Store the result in df
        if df is not None and not df.rdd.isEmpty():
            self.write_to_sql_server(df, table_name=table_name)
            self.migration_log_info(table_name, blob_path)
        else:
            print(f"No data found in blob path: {blob_path}")

    def process_all_tables(self, table_names):
        for table_name in table_names:
            self.process_table(table_name)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Migration").getOrCreate()
    key_vault_scope = "key_vault_scope_migration"
    table_names = ["dbo.Customer", "dbo.Product", "dbo.SalesOrderDetail", "dbo.SalesOrderHeader"]
    databricks_connector = DatabricksConnector(spark, key_vault_scope)
    databricks_connector.process_all_tables(table_names)
    