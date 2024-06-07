import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.storage.blob import ContentSettings
import os
import concurrent.futures  # Added import

class storageCopy:
    def __init__(self, spark):
        self.spark = spark
        
        # Create Source Storage Account Connection String, Access key is stored in Azure Key Vault
        self.source_storage_account_name = "sourcestorageaccount"
        self.source_container_name = "sourcecontainer"
        self.source_key = "sourcekey"
        self.source_access_key = dbutils.secrets.get(scope="keyvaultscope", key=self.source_key)
        self.source_storage_url = f"abfss://{self.source_container_name}@{self.source_storage_account_name}.dfs.core.windows.net"
        self.source_storage_url_http = f"https://{self.source_storage_account_name}.blob.core.windows.net"
        self.source_connection_string = f"DefaultEndpointsProtocol=https;AccountName={self.source_storage_account_name};AccountKey={self.source_access_key};EndpointSuffix=core.windows.net"
        
        # Create Destination Storage Account Connection String, Access key is stored in Azure Key Vault
        self.dest_storage_account_name = "deststorageaccount"
        self.dest_container_name = "destcontainer"
        self.dest_key = "destkey"
        self.dest_access_key = dbutils.secrets.get(scope="keyvaultscope", key=self.dest_key)
        self.dest_storage_url = f"abfss://{self.dest_container_name}@{self.dest_storage_account_name}.dfs.core.windows.net"
        self.dest_storage_url_http = f"https://{self.dest_storage_account_name}.blob.core.windows.net"
        self.dest_connection_string = f"DefaultEndpointsProtocol=https;AccountName={self.dest_storage_account_name};AccountKey={self.dest_access_key};EndpointSuffix=core.windows.net"
        
        self.recurse = True
        self.set_config()
        
    def set_config(self):
        self.spark.conf.set("fs.azure.account.key." + self.source_storage_account_name + ".blob.core.windows.net", self.source_access_key)
        self.spark.conf.set("fs.azure.account.key." + self.dest_storage_account_name + ".blob.core.windows.net", self.dest_access_key)
        
    # Copy a folder from one storage account to another storage account
    def copy_folder(self, source_folder, destination_folder):
        try:
            source_folder_path = f"{self.source_storage_url}/{source_folder}"
            destination_folder_path = f"{self.dest_storage_url}/{destination_folder}"
            dbutils.fs.cp(source_folder_path, destination_folder_path, recurse=self.recurse)
            print(f"Folder {source_folder} copied to {destination_folder}")
        except Exception as e:
            print(f"Error in copying folder {source_folder} to {destination_folder}: {e}")
            raise e
        
    def parallel_copy_folder(self, source_folder, destination_folder):
        # Get the list of subfolders and files in the source folder
        source_folder_path = f"{self.source_storage_url}/{source_folder}"
        contents = dbutils.fs.ls(source_folder_path)
        file_paths = []
        # Iterate through the list of subfolders and files
        for items in contents:
            file_paths.append(items.path)
        # Create a thread pool to copy the files in parallel with a max of 10 threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            for file_path in file_paths:
                dest_file_path = f"{destination_folder}/{os.path.basename(file_path)}"
                executor.submit(self.copy_file, file_path, dest_file_path)

    def copy_file(self, source_file_path, dest_file_path):
        try:
            dbutils.fs.cp(source_file_path, dest_file_path)
            print(f"File {source_file_path} copied to {dest_file_path}")
        except Exception as e:
            print(f"Error in copying file {source_file_path} to {dest_file_path}: {e}")
            raise e

if __name__ == "__main__":
    spark = SparkSession.builder.appName("AzureStorageMigration").getOrCreate()

    storage_copy = storageCopy(spark)
    source_folder_path = "sourcefolder"
    destination_folder_path = "destinationfolder"
    storage_copy.copy_folder(source_folder_path, destination_folder_path)

    spark.stop()
