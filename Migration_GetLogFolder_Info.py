from pyspark.sql import SparkSession, Row
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
import os

class BlobFolderInfoLogger:
    def __init__(self, storage_account, container_name, sql_mi_jdbc_url, sql_mi_properties):
        self.storage_account = storage_account
        self.container_name = container_name
        self.sql_mi_jdbc_url = sql_mi_jdbc_url
        self.sql_mi_properties = sql_mi_properties
        self.spark = SparkSession.builder.appName("BlobFolderInfoLogger").getOrCreate()
        self.blob_service_client = BlobServiceClient(account_url=f"https://{storage_account}.blob.core.windows.net", credential=DefaultAzureCredential())

    def get_row_count(self, file_path):
        df = self.spark.read.parquet(file_path)
        return df.count()

    def get_folder_info(self):
        container_client = self.blob_service_client.get_container_client(self.container_name)
        folder_info = {}

        for blob in container_client.list_blobs():
            folder_path = os.path.dirname(blob.name)
            file_name = os.path.basename(blob.name)
            file_size_mb = blob.size / (1024 * 1024)
            file_path = f"wasbs://{self.container_name}@{self.storage_account}.blob.core.windows.net/{blob.name}"
            file_rows = self.get_row_count(file_path)

            if folder_path not in folder_info:
                folder_info[folder_path] = {
                    "files": [{
                        "file_name": file_name,
                        "file_size_mb": file_size_mb,
                        "file_rows": file_rows
                    }],
                    "total_rows": file_rows,
                    "total_size_gb": file_size_mb / 1024
                }
            else:
                folder_info[folder_path]["files"].append({
                    "file_name": file_name,
                    "file_size_mb": file_size_mb,
                    "file_rows": file_rows
                })
                folder_info[folder_path]["total_rows"] += file_rows
                folder_info[folder_path]["total_size_gb"] += file_size_mb / 1024

        return folder_info

    def log_folder_info(self, folder_info):
        rows = []
        for folder_path, info in folder_info.items():
            for file in info["files"]:
                rows.append(Row(
                    folder_path=folder_path,
                    file_name=file["file_name"],
                    file_size_mb=file["file_size_mb"],
                    file_rows=file["file_rows"],
                    total_rows_in_folder=info["total_rows"],
                    folder_size_gb=info["total_size_gb"]
                ))

        df = self.spark.createDataFrame(rows)
        df.write \
            .format("jdbc") \
            .option("url", self.sql_mi_jdbc_url) \
            .option("dbtable", "folder_info_log") \
            .option("user", self.sql_mi_properties["user"]) \
            .option("password", self.sql_mi_properties["password"]) \
            .mode("append") \
            .save()

if __name__ == "__main__":
    storage_account = "your_storage_account"
    container_name = "your_container_name"
    sql_mi_jdbc_url = "your_sql_mi_jdbc_url"
    sql_mi_properties = {
        "user": "your_username",
        "password": "your_password"
    }

    logger = BlobFolderInfoLogger(storage_account, container_name, sql_mi_jdbc_url, sql_mi_properties)
    folder_info = logger.get_folder_info()
    logger.log_folder_info(folder_info)
