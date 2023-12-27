from pyspark.sql import SparkSession, Row, DataFrame
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
import os
from functools import reduce
import time


class BlobFolderInfo:
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

    # Get all folders info from Azure Data Lake Storage Gen2 Blob Storage Container
    def get_blob_folders(self, blob_container_name):
        blob_service_client = BlobServiceClient(account_url=self.blob_storage_url, credential=self.blob_account_key)
        container_client = blob_service_client.get_container_client(blob_container_name)
        blob_folders = []
        for blob in container_client.list_blobs():
            blob_folders.append(blob.name)
        return blob_folders
    
    # Get all folders info from Azure Data Lake Storage Gen2 Blob Storage Container but limit to 3 folders
    def get_blob_folders_limit(self, blob_container_name):
        blob_service_client = BlobServiceClient(account_url=self.blob_storage_url, credential=self.blob_account_key)
        container_client = blob_service_client.get_container_client(blob_container_name)
        blob_folders = []
        for blob in container_client.list_blobs():
            blob_folders.append(blob.name)
        return blob_folders[:3]
    
    # Get blob folder size from Azure Data Lake Storage Gen2 Blob Storage Container
    def get_blob_folder_size(self, blob_folder_name):
        blob_service_client = BlobServiceClient(account_url=self.blob_storage_url, credential=self.blob_account_key)
        container_client = blob_service_client.get_container_client(self.blob_container_name)
        blob_folder_size = 0
        for blob in container_client.list_blobs(name_starts_with=blob_folder_name):
            blob_folder_size += blob.size
        return blob_folder_size
    
    
    #Get all folders info from Azure Data Lake Storage Gen2 Blob Storage Container and size of each folder
    def get_blob_folders_size(self, blob_container_name):
        blob_service_client = BlobServiceClient(account_url=self.blob_storage_url, credential=self.blob_account_key)
        container_client = blob_service_client.get_container_client(blob_container_name)
        blob_folders = []
        for blob in container_client.list_blobs():
            blob_folders.append(blob.name)
        blob_folders_size = []
        for blob_folder in blob_folders:
            blob_folders_size.append((blob_folder, self.get_blob_folder_size(blob_folder)))
        return blob_folders_size
    
if __name__ == "__main__":
    spark = SparkSession.builder.appName("Migration_ParallelProcessing").getOrCreate()
    key_vault_scope = "key_vault_scope_migration"
    databricks_connector = DatabricksConnector(spark, key_vault_scope)
    blob_container_name = dbutils.secrets.get(scope=key_vault_scope, key="blob-container-name")
    All_blob_folders = databricks_connector.get_blob_folders(blob_container_name)
    Limit_blob_folders = databricks_connector.get_blob_folders_limit(blob_container_name)
    print(All_blob_folders)
    print(Limit_blob_folders)
    print(blob_folders)
    spark.stop()
    