from pyspark.sql import SparkSession, Row
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
import traceback
import os
from dbuutils import DBUtils

class DataMigrator:
    def __init__(self):
        self.spark = SparkSession.builder.appName("DataMigration").getOrCreate()
        self.key_vault_scope = "key-vault-scope"
        self.alpha_storage_account = self.get_secret(scope = "key-vault-scope", key = "alpha-storage-account-name")
        self.alpha_container = self.get_secret(scope = "key-vault-scope", key = "alpha-container-name")
        self.beta_storage_account = self.get_secret(scope = "key-vault-scope", key = "beta-storage-account-name")
        self.beta_container = self.get_secret(scope = "key-vault-scope", key = "beta-storage-container-name")
        self.jdbc_hostname = self.get_secrets(scope = "key-vault-scope", key = "sql-server-hostname")
        self.jdbc_database = self.get_secrets(scope = "key-vault-scope", key = "sql-database-name")
        self.jdbc_username = self.get_secrets(scope = "key-vault-scope", key = "sql-username")
        self.jdbc_password = self.get_secrets(scope = "key-vault-scope", key = "sql-password")
        
    # Mount storage containers, if not already mounted, if mounted print message
    def mount_storage_containers(self):
        try:
            dbutils.fs.ls(f"/mnt/{self.alpha_container}")
            print(f"/mnt/{self.alpha_container} is already mounted")
        except Exception as e:
            print(f"Mounting /mnt/{self.alpha_container}")
            dbutils.fs.mount(
                source = f"wasbs://{self.alpha_container}@{self.alpha_storage_account}",
                mount_point = f"/mnt/{self.alpha_container}",
                extra_configs = self.get_storage_account_config(self.alpha_storage_account)
            )
        try:
            dbutils.fs.ls(f"/mnt/{self.beta_container}")
            print(f"/mnt/{self.beta_container} is already mounted")
        except Exception as e:
            print(f"Mounting /mnt/{self.beta_container}")
            dbutils.fs.mount(
                source = f"wasbs://{self.beta_container}@{self.beta_storage_account}",
                mount_point = f"/mnt/{self.beta_container}",
                extra_configs = self.get_storage_account_config(self.beta_storage_account)
            )
    # List all folders in a root folder of a storage container and print the list
    def list_folders(self, container, root_folder):
        folders = []
        for folder in dbutils.fs.ls(f"/mnt/{container}/{root_folder}"):
            folders.append(folder.name)
        return folders
        print(folders)
        
    datamigrator = DataMigrator()
    datamigrator.mount_storage_containers()
    alpha_folders = datamigrator.list_folders(datamigrator.alpha_container, "data")
    
    