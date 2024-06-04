from pyspark.sql import SparkSession
import zipfile
import io
import os
from azure.storage.blob import BlobServiceClient

# Initialize Spark session
spark = SparkSession.builder.appName("UnzipAndCopyFiles").getOrCreate()

class StorageCopy:
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
        
        self.set_config()
        
    def set_config(self):
        self.spark.conf.set("fs.azure.account.key." + self.source_storage_account_name + ".blob.core.windows.net", self.source_access_key)
        self.spark.conf.set("fs.azure.account.key." + self.dest_storage_account_name + ".blob.core.windows.net", self.dest_access_key)

def unzip_and_copy_files(storage_copy_instance, zip_blob_name, destination_folder):
    source_blob_service_client = BlobServiceClient.from_connection_string(storage_copy_instance.source_connection_string)
    source_container_client = source_blob_service_client.get_container_client(storage_copy_instance.source_container_name)
    
    dest_blob_service_client = BlobServiceClient.from_connection_string(storage_copy_instance.dest_connection_string)
    dest_container_client = dest_blob_service_client.get_container_client(storage_copy_instance.dest_container_name)
    
    # Download the ZIP file from source storage
    zip_blob_client = source_container_client.get_blob_client(zip_blob_name)
    zip_data = io.BytesIO(zip_blob_client.download_blob().readall())
    
    # Extract the ZIP file
    with zipfile.ZipFile(zip_data, 'r') as zip_ref:
        for file_name in zip_ref.namelist():
            file_data = zip_ref.read(file_name)
            dest_blob_client = dest_container_client.get_blob_client(os.path.join(destination_folder, file_name))
            dest_blob_client.upload_blob(file_data, overwrite=True)

def copy_and_unzip_files_from_folder(storage_copy_instance, source_folder, destination_folder):
    source_blob_service_client = BlobServiceClient.from_connection_string(storage_copy_instance.source_connection_string)
    source_container_client = source_blob_service_client.get_container_client(storage_copy_instance.source_container_name)
    
    dest_blob_service_client = BlobServiceClient.from_connection_string(storage_copy_instance.dest_connection_string)
    dest_container_client = dest_blob_service_client.get_container_client(storage_copy_instance.dest_container_name)
    
    # List blobs in the source folder
    blobs = source_container_client.list_blobs(name_starts_with=source_folder)
    
    for blob in blobs:
        if blob.name.endswith('.zip'):
            source_blob_client = source_container_client.get_blob_client(blob.name)
            zip_data = io.BytesIO(source_blob_client.download_blob().readall())
            
            # Extract the ZIP file
            with zipfile.ZipFile(zip_data, 'r') as zip_ref:
                for file_name in zip_ref.namelist():
                    file_data = zip_ref.read(file_name)
                    dest_blob_client = dest_container_client.get_blob_client(os.path.join(destination_folder, file_name))
                    dest_blob_client.upload_blob(file_data, overwrite=True)

if __name__ == "__main__":
    # Example usage
    storage_copy_instance = StorageCopy(spark)
    
    # Unzip a specific ZIP file and copy its contents
    unzip_and_copy_files(storage_copy_instance, 'path/to/your.zip', 'destination/folder')
    
    # Unzip all ZIP files in a source folder and move the contents to a destination folder
    copy_and_unzip_files_from_folder(storage_copy_instance, 'source/folder', 'destination/folder')
