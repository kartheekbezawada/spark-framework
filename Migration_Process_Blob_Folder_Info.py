from pyspark.sql import SparkSession
import dbutils
from azure.storage.blob import BlobServiceClient

class BlobFolderInfo:
    def __init__(self, spark, key_vault_scope):
        self.spark = spark
        self.key_vault_scope = key_vault_scope
        self.blob_account_name = dbutils.secrets.get(scope=self.key_vault_scope, key="blob-storage-account-name")
        self.sas_token = dbutils.secrets.get(scope=self.key_vault_scope, key="blob-sas-token")
        self.blob_container_name = dbutils.secrets.get(scope=self.key_vault_scope, key="blob-container-name")
        self.blob_storage_url = self._get_blob_storage_url()

    def _get_blob_storage_url(self):
        return f"https://{self.blob_account_name}.blob.core.windows.net"

    def get_table_folders_and_sizes(self, container_name, table_depth):
        try:
            blob_service_client = BlobServiceClient(account_url=self.blob_storage_url, credential=self.sas_token)
            container_client = blob_service_client.get_container_client(container_name)
            blob_list = container_client.list_blobs()

            table_folders = {}

            for blob in blob_list:
                # Check if the blob's folder depth matches the table level
                if len(blob.name.split('/')) == table_depth + 1:  # +1 accounts for the blob file itself
                    table_folder_path = '/'.join(blob.name.split('/')[:-1])
                    table_folders[table_folder_path] = table_folders.get(table_folder_path, 0) + blob.size

            # Convert sizes to MB
            for folder in table_folders:
                table_folders[folder] = table_folders[folder] / (1024 * 1024)

            return table_folders
        except Exception as e:
            print(f"Error getting table folders and sizes in container {container_name}: {e}")
            raise e

if __name__ == "__main__":
    spark = SparkSession.builder.appName("AzureBlobFolderAnalysis").getOrCreate()
    key_vault_scope = "key_vault_scope_migration"

    blob_folder_info = BlobFolderInfo(spark, key_vault_scope)
    container_name = dbutils.secrets.get(scope=key_vault_scope, key="blob-container-name")

    # Specify the depth of the table folders in the hierarchy (e.g., 5 for 'folder_1/sub_folder_2/sub_folder_3/sub_folder_4/sub_folder_5')
    table_depth = 5

    table_folders_and_sizes = blob_folder_info.get_table_folders_and_sizes(container_name, table_depth)

    print("Table Folders and their Sizes in MB:")
    for table_folder, size in table_folders_and_sizes.items():
        print(f"Table folder: {table_folder}, Size: {size:.2f} MB")

    spark.stop()
