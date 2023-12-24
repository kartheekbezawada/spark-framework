from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

class DatabricksConnector:
    def __init__(self, spark, key_vault_scope):
        self.spark = spark
        self.key_vault_scope = key_vault_scope
        self.jdbc_hostname = None
        self.jdbc_database = None
        self.jdbc_username = None
        self.jdbc_password = None
        self.blob_account_name = None
        self.blob_account_key = None
        self.blob_container_name = None
 

    def _load_credentials(self):
        # Load credentials from Azure Key Vault
        try:
            self.jdbc_hostname = dbutils.secrets.get(scope=self.key_vault_scope, key="sql-server-hostname")
            self.jdbc_database = dbutils.secrets.get(scope=self.key_vault_scope, key="sql-database-name")
            self.jdbc_username = dbutils.secrets.get(scope=self.key_vault_scope, key="sql-username")
            self.jdbc_password = dbutils.secrets.get(scope=self.key_vault_scope, key="sql-password")
            self.blob_account_name = dbutils.secrets.get(scope=self.key_vault_scope, key="blob-storage-account-name")
            self.blob_account_key = dbutils.secrets.get(scope=self.key_vault_scope, key="blob-storage-account-key")
            self.blob_container_name = dbutils.secrets.get(scope=self.key_vault_scope, key="blob-container-name")
        except Exception as e:
            print(f"Error loading credentials: {e}")
            raise e
        
    def _get_jdbc_url(self):
        return f"jdbc:sqlserver://{self.jdbc_hostname};database={self.jdbc_database};user={self.jdbc_username};password={self.jdbc_password}"
    
    def _get_blob_storage_url(self):
        return f"wasbs://{self.blob_container_name}@{self.blob_account_name}"
    
    def _get_blob_storage_config(self):
        return {
            "fs.azure.account.key.{self.blob_account_name}.blob.core.windows.net": self.blob_account_key
        }
        
    def connect_sql_server(self):
        self._load_credentials()
        jdbc_url = self._get_jdbc_url()
        try:
            df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "dbo.SalesLT.Customer") \
                .option("user", self.jdbc_username) \
                .option("password", self.jdbc_password) \
                .load()
        except AnalysisException as e:
            print(f"Error connecting to SQL Server: {e}")
            raise e
        return df