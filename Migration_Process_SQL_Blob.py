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

    def read_from_sql_server(self, table_name):
        try:
            df = self.spark.read \
                .format("jdbc") \
                .option("url", self._get_jdbc_url()) \
                .option("dbtable", table_name) \
                .option("user", self.jdbc_username) \
                .option("password", self.jdbc_password) \
                .load()
        except AnalysisException as e:
            print(f"Error connecting to SQL Server for table {table_name}: {e}")
            raise e
        return df
    
    #write to blob storage maximun 1000 records per file
    def write_to_blob_storage(self, df, blob_path):
        try:
            df.write \
                .mode("append") \
                .options(**self.blob_storage_config) \
                .option("maxRecordsPerFile", 1000) \
                .parquet(blob_path) 
        except Exception as e:
            print(f"Error writing to blob storage: {e}")
            raise e            
    
    def migrate_data(self, table_names):
        for table_name in table_names:
            df = self.read_from_sql_server(table_name)
            blob_path = f"{self.blob_storage_url}/{table_name.replace('.', '_')}"
            self.write_to_blob_storage(df, blob_path)
            print(f"Data migrated for table: {table_name}") 
    
    def process_all_tables(self, table_names):
        for table_name in table_names:
            df = self.read_from_sql_server(table_name)
            blob_path = f"{self.blob_storage_url}/{table_name.replace('.', '_')}"
            self.write_to_blob_storage(df, blob_path)
            print(f"Data migrated for table: {table_name}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Migration").getOrCreate()
    key_vault_scope = "key_vault_scope_migration"
    table_names = ["dbo.Customer", "dbo.Product", "dbo.SalesOrderDetail", "dbo.SalesOrderHeader"]
    databricks_connector = DatabricksConnector(spark, key_vault_scope)
    databricks_connector.process_all_tables(table_names)
    