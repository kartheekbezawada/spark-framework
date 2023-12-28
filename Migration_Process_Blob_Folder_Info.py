from pyspark.sql import SparkSession
import dbutils
from azure.storage.blob import BlobServiceClient

class BlobFolderInfo:
    def __init__(self, spark, key_vault_scope):
        self.spark = spark
        self.key_vault_scope = key_vault_scope
        self.blob_account_name = dbutils.secrets.get(scope=self.key_vault_scope, key="blob-storage-account-name")
        self.blob_account_key = dbutils.secrets.get(scope=self.key_vault_scope, key="blob-storage-account-key")
        self.blob_container_name = dbutils.secrets.get(scope=self.key_vault_scope, key="blob-container-name")
        self.blob_storage_url = self._get_blob_storage_url()

    def _get_blob_storage_url(self):
        return f"https://{self.blob_account_name}.blob.core.windows.net"

    def get_all_folders(self, container_name):
        try:
            blob_service_client = BlobServiceClient(account_url=self.blob_storage_url, credential=self.blob_account_key)
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
            blob_service_client = BlobServiceClient(account_url=self.blob_storage_url, credential=self.blob_account_key)
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
    
    def get_top_n_folders_by_size(self, container_name, n):
        try:
            # Get folder sizes in MB
            folder_sizes_in_mb = self.get_folders_size_in_mb(container_name)

            # Sort the folders by size and get the top 'n'
            top_folders = sorted(folder_sizes_in_mb.items(), key=lambda x: x[1], reverse=True)[:n]

            return top_folders
        except Exception as e:
            print(f"Error getting top {n} folders in container {container_name}: {e}")
            raise e
    
    
if __name__ == "__main__":
    spark = SparkSession.builder.appName("Migration").getOrCreate()
    key_vault_scope = "key_vault_scope_migration"

    # Initialize BlobFolderInfo instance
    blob_folder_info = BlobFolderInfo(spark, key_vault_scope)

    # Retrieve the name of the container from key vault
    container_name = dbutils.secrets.get(scope=key_vault_scope, key="blob-container-name")

    # Get all folder names in the container
    folder_list = blob_folder_info.get_all_folders(container_name)
    print("Folders in the container:")
    for folder in folder_list:
        print(folder)

    # Get sizes of all folders in MB
    folder_sizes_in_mb = blob_folder_info.get_folders_size_in_mb(container_name)
    print("\nSizes of folders in MB:")
    for folder, size in folder_sizes_in_mb.items():
        print(f"{folder}: {size:.2f} MB")

    # Specify the number of top folders to retrieve
    top_n = 5  # Example: top 5 folders

    # Get the top 'n' folders by size
    top_folders = blob_folder_info.get_top_n_folders_by_size(container_name, top_n)
    print(f"Top {top_n} folders by size:")
    for folder, size in top_folders:
        print(f"{folder}: {size:.2f} MB")

    spark.stop()
    