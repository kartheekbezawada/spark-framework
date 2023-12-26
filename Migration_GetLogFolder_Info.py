from pyspark.sql import SparkSession, Row
import os
import dbutils

class BlobFolderInfoLogger:
    def __init__(self, spark, key_vault_scope):
        # Use the existing Spark session
        spark = SparkSession.builder.appName("BlobFolderInfoLogger").getOrCreate()
        self.key_vault_scope = "key_vault_scope"
        self.jdbc_hostname = dbutils.secrets.get(scope="key_vault_scope", key="sql-server-hostname")
        self.jdbc_database = dbutils.secrets.get(scope="key_vault_scope", key="sql-database-name")
        self.jdbc_username = dbutils.secrets.get(scope="key_vault_scope", key="sql-username")
        self.jdbc_password = dbutils.secrets.get(scope="key_vault_scope", key="sql-password")
        self.blob_account_name = dbutils.secrets.get(scope="key_vault_scope", key="blob-storage-account-name")
        self.blob_account_key = dbutils.secrets.get(scope="key_vault_scope", key="blob-storage-account-key")
        self.blob_container_name = dbutils.secrets.get(scope="key_vault_scope", key="blob-container-name")

    def _get_blob_storage_url(self):
        return f"wasbs://{self.blob_container_name}@{self.blob_account_name}.blob.core.windows.net"

    def _get_blob_storage_config(self):
        return {f"fs.azure.account.key.{self.blob_account_name}.blob.core.windows.net": self.blob_account_key}

    def _get_jdbc_url(self):
        return f"jdbc:sqlserver://{self.jdbc_hostname};database={self.jdbc_database}"

    def get_row_count(self, file_path):
        df = self.spark.read.parquet(file_path)
        return df.count()
    
    def _get_file_size_mb(self, item):
        return item.size / (1024 * 1024)

    def _get_file_name(self, file_path):
        return os.path.basename(file_path)

    def get_folder_info(self):
        # Setting the configuration for Blob Storage
        self.spark.conf.setAll(self._get_blob_storage_config())

        # Accessing Blob Storage directly via WASB
        blob_storage_url = self._get_blob_storage_url()
        # Assuming the container contains only Parquet files or you're only interested in Parquet files
        files_and_dirs = self.spark.read.format("parquet").load(blob_storage_url).rdd.map(lambda x: x._jvm.org.apache.hadoop.fs.Path(x.getPath.toString())).collect()
        folder_info = {}

        for file_path in files_and_dirs:
            file_size_mb = self._get_file_size_mb(file_path)  # This needs a method to get the size from WASB path
            file_rows = self.get_row_count(file_path)
            folder_path, file_name = os.path.split(file_path)

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
            .option("url", self._get_jdbc_url()) \
            .option("dbtable", "folder_info_log") \
            .option("user", self.jdbc_username) \
            .option("password", self.jdbc_password) \
            .mode("append") \
            .save()

if __name__ == "__main__":
    logger = BlobFolderInfoLogger(spark, key_vault_scope)
    folder_info = logger.get_folder_info()
    #Print the folder information for testing purposes
    print("Folder Information:", folder_info)
    logger.log_folder_info(folder_info)
    spark.stop()