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

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Migration").getOrCreate()
    key_vault_scope = "key_vault_scope_migration"
    blob_folder_info = BlobFolderInfo(spark, key_vault_scope)
    container_name = "container_name"
    folder_list = blob_folder_info.get_all_folders(container_name)
    print(folder_list)
