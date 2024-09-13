import os
import uuid
from datetime import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
import base64
import logging

class EnrichIngestion:
    def __init__(self, spark):
        self.spark = spark
        self.logger = logging.getLogger("EnrichIngestion")
        logging.basicConfig(level=logging.INFO)

        self.applicationName = applicationName
        self.askID = askID
        self.extractType = extractType

        # Environment variables
        self.instance = self._get_env_variable("instance")
        self.loc = self._get_env_variable("loc")
        self.env = self._get_env_variable("env")

        # Key Vault Secret configuration
        self.key_vault_scope = f"kv-sa-lnd-{self.env}-{self.loc}-{self.instance}"

        # Landing account configuration
        self.landing_account_name = f"sa-lnd-{self.env}-{self.loc}-{self.instance}"
        self.landing_account_key = self._get_secret(f"sa-lnd-{self.env}-{self.loc}-{self.instance}")
        self.landing_container = "landing"

        # Enrich account configuration
        self.enrich_account_name = f"sa-enrich-{self.env}-{self.loc}-{self.instance}"
        self.enrich_account_key = self._get_secret(f"sa-enrich-{self.env}-{self.loc}-{self.instance}")
        self.enrich_container = "enrich"

        # Azure Synapse configuration JDBC URL
        self.synUser = "sqladmin"
        self.synPass = self._get_secret("synapse-sqladmin")
        self.synServer = f"jdbc:sqlserver://{self.env}-{self.loc}-{self.instance}.database.windows.net"
        self.synDatabase = "synapse-db"
        self.jdbc_url = f"{self.synServer};database={self.synDatabase};user={self.synUser};password={self.synPass};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

        # Log Metadata
        self.job_id = str(uuid.uuid4())
        self.rows_processed = 0
        self.job_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.job_end_time = None
        self.job_date = datetime.now().strftime("%Y-%m-%d")

        # Set Spark configurations for storage
        self._set_storage_config()
        self.landing_storage_url = f"abfss://{self.landing_container}@{self.landing_account_name}.dfs.core.windows.net"
        self.enrich_storage_url = f"abfss://{self.enrich_container}@{self.enrich_account_name}.dfs.core.windows.net"

    def _get_env_variable(self, var_name):
        value = os.getenv(var_name)
        if not value:
            self.logger.error(f"Environment variable '{var_name}' is not set.")
            raise EnvironmentError(f"Environment variable '{var_name}' is required.")
        return value

    def _get_secret(self, secret_name):
        try:
            return dbutils.secrets.get(scope=self.key_vault_scope, key=secret_name)
        except Exception as e:
            self.logger.error(f"Failed to retrieve secret '{secret_name}': {e}")
            raise

    def _set_storage_config(self):
        try:
            self.spark.conf.set(f"fs.azure.account.key.{self.landing_account_name}.blob.core.windows.net", self.landing_account_key)
            self.spark.conf.set(f"fs.azure.account.key.{self.enrich_account_name}.blob.core.windows.net", self.enrich_account_key)
            self.logger.info("Storage configuration set successfully.")
        except Exception as e:
            self.logger.error(f"Failed to set storage configuration: {e}")
            raise

    def synapse_connection(self):
        try:
            df = self.spark.read \
                .format("com.databricks.spark.sqldw") \
                .option("url", self.jdbc_url) \
                .option("dbtable", "dbo.salary") \
                .load()
            if df.count() > 0:
                self.logger.info("Connection to Synapse is successful.")
            else:
                self.logger.warning("Connection to Synapse is successful but no data was retrieved.")
        except Exception as e:
            self.logger.error(f"Synapse connection failed: {e}")
            raise

    def get_format_options(self, cloudFileFormat):
        format_options = {
            "csv": self._csv_options,
            "parquet": self._parquet_options,
            "json": self._json_options,
            "xml": self._xml_options
        }.get(cloudFileFormat, None)

        if not format_options:
            self.logger.error(f"Invalid cloud file format: {cloudFileFormat}")
            raise ValueError(f"Invalid cloud file format: {cloudFileFormat}")
        
        return format_options()

    def _csv_options(self):
        return {
            "header": "true",
            "inferSchema": "true",
            "delimiter": ",",
            "multiLine": "true"
        }

    def _parquet_options(self):
        return {
            "header": "true",
            "inferSchema": "true"
        }

    def _json_options(self):
        return {
            "header": "true",
            "inferSchema": "true"
        }

    def _xml_options(self):
        return {
            "rowTag": "record"
        }

    def add_metadata(self, df):
        return df.withColumn("md_process_id", F.expr("uuid()")) \
                 .withColumn("md_source_path", F.input_file_name()) \
                 .withColumn("md_source_ts", F.col("_metadata.file_modification_time")) \
                 .withColumn("md_created_ts", F.current_timestamp())

    def read_stream_data(self, config):
        try:
            schema_location = f"{self.enrich_storage_url}/{config['schemaPath']}"
            landing_location = f"{self.landing_storage_url}/{config['landingReadPath']}"
            format_options = self.get_format_options(config['cloudFileFormat'])

            return (
                self.spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", config['cloudFileFormat'])
                .option("multiLine", "True")
                .option("cloudFiles.inferColumnTypes", "True")
                .option("recursiveFileLookup", "True")
                .option("cloudFiles.schemaLocation", schema_location)
                .options(**format_options)
                .load(landing_location)
            )
        except Exception as e:
            self.logger.error(f"Error reading stream data: {e}")
            raise

    def rename_invalid_columns(self, df):
        invalid_characters = [' ', '.', ',', ';', ':', '!', '@']
        for col in df.columns:
            new_col = col
            for char in invalid_characters:
                new_col = new_col.replace(char, '_')
            df = df.withColumnRenamed(col, new_col)
        return df

    def encrypt_val(self, clear_text, MASTER_KEY):
        try:
            if clear_text:
                cipher = AES.new(bytes(MASTER_KEY, 'ascii'), AES.MODE_CBC)
                cipher_text = cipher.encrypt(pad(clear_text.encode('UTF-8'), AES.block_size))
                textBase64 = base64.b64encode(cipher_text)
                return textBase64.decode('UTF-8')
            return clear_text
        except Exception as e:
            self.logger.error(f"Error encrypting value: {e}")
            raise

    def is_valid_pii_category(self, piicategory):
        valid_categories = [
            "colleague-secret", 
            "colleague-confidential",
            "customer-secret",
            "customer-confidential",
            "supplier-secret",
            "supplier-confidential"
        ]
        return piicategory in valid_categories

    def enforce_pii(self, df, config, encryptionKey):
        if self.is_valid_pii_category(config['pii_category']) and config.get('pii_columns'):
            for columnName in config['pii_columns']:
                if columnName in df.columns:
                    df = df.withColumn(columnName, F.expr(f"Python:DLTTableCreator().encrypt_val({columnName},'{encryptionKey}')"))
                else:
                    self.logger.warning(f"Column {columnName} not found in DataFrame.")
        return df

    def write_stream_to_stage(self, df, config):
        try:
            checkPointLocation = f"{self.enrich_storage_url}/{config['checkPointReadPath']}"    
            stageTablePath = f"{self.enrich_storage_url}/{config['stageTablePath']}"

            query = (
                df.writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", checkPointLocation)
                .trigger(availableNow=True)
                .start(stageTablePath)
            )
            query.awaitTermination()
            self.logger.info("Data written to stage table successfully.")
        except Exception as e:
            self.logger.error(f"Error writing stream to stage: {e}")
            raise

    def read_stage_table(self, config):
        try:
            stageTablePath = f"{self.enrich_storage_url}/{config['stageTablePath']}"
            df = self.spark.read.format("delta").load(stageTablePath)
            row_count = df.count()
            self.logger.info(f"Number of rows read from stage table: {row_count}")
            return df
        except Exception as e:
            self.logger.error(f"Error reading stage table: {e}")
            raise

    def write_to_delta_table(self, df, config):
        deltaTablePath = f"{self.enrich_storage_url}/{config['deltaTablePath']}"
        write_mode = config['write_mode']
        partition_columns = config.get('partition_columns')

        try:
            if partition_columns:
                df.write.format("delta") \
                    .mode(write_mode) \
                    .partitionBy(partition_columns) \
                    .save(deltaTablePath)
            else:
                df.write.format("delta") \
                    .mode(write_mode) \
                    .save(deltaTablePath)
            self.rows_processed = df.count()
            self.logger.info(f"{self.rows_processed} rows processed and written to Delta table.")
        except Exception as e:
            self.logger.error(f"Error writing to Delta table: {e}")
            raise

    def log_metadata(self):
        schema = StructType([
            StructField("jobID", StringType(), False),
            StructField("applicationName", StringType(), False),
            StructField("askID", StringType(), False),
            StructField("extractType", StringType(), False),
            StructField("entityName", StringType(), False),
            StructField("databaseName", IntegerType(), False),
            StructField("rowsProcessed", IntegerType(), False),
            StructField("jobStartTime", StringType(), False),
            StructField("jobEndTime", StringType(), False),
            StructField("jobRunDate", StringType(), False),
            StructField("sourceFilePath", StringType(), False),
            StructField("frequency", StringType(), False)
        ])
        metadata = [(self.job_id, self.applicationName, self.askID, self.extractType, self.rows_processed, self.job_start_time, self.job_end_time, self.job_date, self.sourceFilePath, self.frequency)]
        metadata_df = self.spark.createDataFrame(metadata, schema)
        return metadata_df

    def write_metadata_to_synapse(self, metadata_df):
        try:
            metadata_df.write \
                .format("com.databricks.spark.sqldw") \
                .option("url", self.jdbc_url) \
                .option("dbtable", "dbo.metadata") \
                .mode("append") \
                .save()
            self.logger.info("Metadata written to Synapse table successfully.")
        except Exception as e:
            self.logger.error(f"Error writing metadata to Synapse table: {e}")
            raise

    def create_unity_catalog_tables(self, config):
        catalougeName = f"core{self.env}"
        schemaName = config['databaseName']
        tableName = config['tableName']
        data_path = f"{self.enrich_storage_url}/{config['deltaTablePath']}" 
        
        try:
            if not self._catalog_exists(catalougeName):
                self.spark.sql(f"CREATE CATALOG {catalougeName}")
                self.logger.info(f"Catalog {catalougeName} created.")
            
            if not self._schema_exists(catalougeName, schemaName):
                self.spark.sql(f"CREATE SCHEMA {catalougeName}.{schemaName}")
                self.logger.info(f"Schema {schemaName} created.")

            if not self._table_exists(catalougeName, schemaName, tableName):
                self.spark.sql(f"CREATE TABLE {catalougeName}.{schemaName}.{tableName} USING DELTA LOCATION '{data_path}'")
                self.logger.info(f"Table {tableName} created.")

            if not self._table_exists(catalougeName, schemaName, f"vw_{tableName}"):
                self.spark.sql(f"CREATE VIEW {catalougeName}.{schemaName}.vw_{tableName} AS SELECT * FROM {catalougeName}.{schemaName}.{tableName}")
                self.logger.info(f"View vw_{tableName} created.")
        except Exception as e:
            self.logger.error(f"Error creating Unity catalog tables: {e}")
            raise

    def _catalog_exists(self, catalougeName):
        return self.spark.sql(f"SHOW CATALOGS LIKE '{catalougeName}'").count() > 0

    def _schema_exists(self, catalougeName, schemaName):
        return self.spark.sql(f"SHOW SCHEMAS IN {catalougeName} LIKE '{schemaName}'").count() > 0

    def _table_exists(self, catalougeName, schemaName, tableName):
        return self.spark.sql(f"SHOW TABLES IN {catalougeName}.{schemaName} LIKE '{tableName}'").count() > 0

    def grant_permission_on_uc_tables(self, config):
        catalougeName = f"core{self.env}"
        schemaName = config['databaseName']
        tableName = config['tableName']
        dataDomain = config['dataDomain']
        securityClassification = config['securityClassification']
        
        try:
            env_prefix = 'nonprod' if self.env in ['dev', 'test', 'stage'] else 'prod'
            securityGroup = 'ag' if securityClassification in ['confidential', 'secret'] else 'rg'
            
            self.spark.sql(f"GRANT SELECT ON TABLE {catalougeName}.{schemaName}.{tableName} TO sg-{securityGroup}-data-{env_prefix}-{dataDomain}-{securityClassification}-read")
            self.logger.info(f"Permission granted to sg-{securityGroup}-data-{env_prefix}-{dataDomain}-{securityClassification}-read on {catalougeName}.{schemaName}.{tableName}.")
        except Exception as e:
            self.logger.error(f"Error granting permission on Unity catalog tables: {e}")
            raise
            
    def create_delta_table(self, config):
        try:
            df = self.read_stream_data(config)
            df = self.rename_invalid_columns(df)
            df = self.add_metadata(df)
            df = self.enforce_pii(df, config, self.landing_account_key)
            self.write_stream_to_stage(df, config)
            df = self.read_stage_table(config)
            self.write_to_delta_table(df, config)
            metadata_df = self.log_metadata()
            self.write_metadata_to_synapse(metadata_df)
            self.create_unity_catalog_tables(config)
            self.grant_permission_on_uc_tables(config)
        except Exception as e:
            self.logger.error(f"Failed to create Delta table: {e}")
            raise

if __name__ == "__main__":
    spark = SparkSession.builder.appName("EnrichIngestion").getOrCreate()

    # Example configuration
    config = {
        'databaseName': 'databaseName',
        'tableName': 'tableName',
        'cloudFileFormat': 'csv',
        'landingReadPath': 'path_to_data_in_landing',
        'checkPointReadPath': 'checkPointReadPath',
        'schemaPath': 'schemaPath',
        'stageTablePath': 'stageTablePath',
        'deltaTablePath': 'deltaTablePath',
        'write_mode': 'append',
        'dataDomain': 'business',
        'schemaName': 'schemaName',
        'pii_category': 'colleague-secret',
        'pii_columns': ['col1', 'col2'],
        'securityClassification': 'confidential',
        'partition_columns': ['col1', 'col2'],
        'convert_to_predefined_sceham': True, # Options: True, False
        'predefined_schema': {
            'columns': [
                {'column_name': 'col1', 'column_type': 'string', 'nullable': True},
                {'column_name': 'col2', 'column_type': 'string', 'nullable': True},
                {'column_name': 'col3', 'column_type': 'string', 'nullable': False},
                {'column_name': 'col4', 'column_type': 'string', 'nullable': False}
            ],
        'new_column_handling': 'Safe',  # Options: 'Auto', 'Safe', 'N'
        'new_columns': [{'column_name': 'col5', 'column_type': 'string', 'nullable': True},
                        {'column_name': 'col5', 'column_type': 'string', 'nullable': True}] }
        'new_column_identified_date': '2022-01-01'
        }

    ingestion = EnrichIngestion(spark, applicationName="MyApp", askID="askID123", extractType="Full")
    df = ingestion.read_stream_data(config)  # Assume this method exists
    df = ingestion.dynamic_schema_management(df, config)
    if df:
        ingestion.write_stream_to_stage(df, config)  # Continue processing