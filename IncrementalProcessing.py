from pyspark.sql import SparkSession
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

class DataMigrator:
    def __init__(self):
        self.spark = SparkSession.builder.appName("DataMigration").getOrCreate()
        self.alpha_container = "alpha-container-name"
        self.beta_container = "beta-container-name"
        self.alpha_storage_account = "alpha-storage-account-name"
        self.beta_storage_account = "beta-storage-account-name"
        self.mount_points = [f"/mnt/{self.alpha_container}", f"/mnt/{self.beta_container}"]
        self.connect_to_azure_storage()

    def connect_to_azure_storage(self):
        try:
            credential = DefaultAzureCredential()
            self.blob_service_client_alpha = BlobServiceClient(
                account_url=f"https://{self.alpha_storage_account}.blob.core.windows.net",
                credential=credential
            )
            self.blob_service_client_beta = BlobServiceClient(
                account_url=f"https://{self.beta_storage_account}.blob.core.windows.net",
                credential=credential
            )
        except Exception as e:
            print(f"Error connecting to Azure Storage: {str(e)}")
            raise e

    def list_folders(self, container_name, prefix=None):
        try:
            container_client = (
                self.blob_service_client_alpha.get_container_client(container_name)
                if container_name == self.alpha_container
                else self.blob_service_client_beta.get_container_client(container_name)
            )
            blob_list = container_client.list_blobs(name_starts_with=prefix)
            folders = set()

            for blob in blob_list:
                if "/" in blob.name:
                    folder_name = blob.name.split("/")[0]
                    folders.add(folder_name)

            return list(folders)
        except Exception as e:
            print(f"Error listing folders in {container_name}: {str(e)}")
            return []

    def mount_containers(self):
        for mount_point in self.mount_points:
            try:
                dbutils.fs.ls(mount_point)
                print(f"{mount_point} is already mounted")
            except Exception as e:
                print(f"Mounting {mount_point}")
                dbutils.fs.mount(
                    source=f"wasbs://{mount_point}@{self.alpha_storage_account}",
                    mount_point=mount_point,
                    extra_configs={"<your_storage_account_config>": "<your_config_value>"}
                )

if __name__ == "__main__":
    datamigrator = DataMigrator()
    datamigrator.mount_containers()

    alpha_folders = datamigrator.list_folders(datamigrator.alpha_container, prefix="data")
    beta_folders = datamigrator.list_folders(datamigrator.beta_container, prefix="data")

    print("Alpha Folders:", alpha_folders)
    print("Beta Folders:", beta_folders)
