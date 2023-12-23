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
        self.cosmos_endpoint = None
        self.cosmos_key = None
        self.synapse_workspace = None
        self.synapse_access_key = None
        self._load_credentials()

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
            self.cosmos_endpoint = dbutils.secrets.get(scope=self.key_vault_scope, key="cosmos-endpoint")
            self.cosmos_key = dbutils.secrets.get(scope=self.key_vault_scope, key="cosmos-key")
            self.synapse_workspace = dbutils.secrets.get(scope=self.key_vault_scope, key="synapse-workspace-url")
            self.synapse_access_key = dbutils.secrets.get(scope=self.key_vault_scope, key="synapse-access-key")
        except Exception as e:
            print(f"Error loading credentials: {e}")

    def connect_sql_server(self, table_name):
        jdbc_url = f"jdbc:sqlserver://{self.jdbc_hostname}:1433;database={self.jdbc_database}"
        connection_properties = {
            "user": self.jdbc_username,
            "password": self.jdbc_password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }
        try:
            df = self.spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
            print("Connection to SQL Server established")
            return df
        except AnalysisException as ae:
            print("No successful connection to SQL Server: ", ae)

    def connect_blob_storage(self, file_name):
        self.spark.conf.set(
            f"fs.azure.account.key.{self.blob_account_name}.blob.core.windows.net",
            self.blob_account_key)
        try:
            df_blob = self.spark.read.format("csv").option("header", "true").load(
                f"wasbs://{self.blob_container_name}@{self.blob_account_name}.blob.core.windows.net/{file_name}")
            print("Connection to Azure Blob Storage established")
            return df_blob
        except AnalysisException as ae:
            print("No successful connection to Azure Blob Storage: ", ae)

    def connect_cosmos_db(self, database_name, container_name):
        try:
            df_cosmos = self.spark.read.format("com.azure.cosmos.spark").option("endpoint", self.cosmos_endpoint).option("key", self.cosmos_key).option("database", database_name).option("container", container_name).load()
            print("Connection to Azure Cosmos DB established")
            return df_cosmos
        except AnalysisException as ae:
            print("No successful connection to Azure Cosmos DB: ", ae)

    def connect_synapse_analytics(self, synapse_table):
        synapse_url = f"{self.synapse_workspace}/sql/ondemand?database={self.jdbc_database}"
        try:
            df_synapse = self.spark.read.format("com.databricks.spark.sqldw").option("url", synapse_url).option("forwardSparkAzureStorageCredentials", "true").option("dbTable", synapse_table).option("tempDir", f"abfss://{self.blob_container_name}@{self.blob_account_name}.dfs.core.windows.net/tempDir").load()
            print("Connection to Azure Synapse Analytics established")
            return df_synapse
        except AnalysisException as ae:
            print("No successful connection to Azure Synapse Analytics: ", ae)


# Usage
spark = SparkSession.builder.appName("DatabricksConnectSQLBlob").getOrCreate()
connector = DatabricksConnector(spark, "your-keyvault-scope")

# Connect to SQL Server
sql_df = connector.connect_sql_server("yourTableName")

# Connect to Blob Storage
blob_df = connector.connect_blob_storage("yourFileName.csv")

# Connect to Cosmos DB
cosmos_df = connector.connect_cosmos_db("yourDatabaseName", "yourContainerName")

# Connect to Synapse Analytics
synapse_df = connector.connect_synapse_analytics("yourSynapseTable")

spark.stop()