import sys
import pyspark
import dlt
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
import base64
from pyspark.sql.types import *
import pyspark.sql.functions as F

class DLTTableCreator:
    def __init__(self, sourceSystem, interfaceID, databaseName, fileFormat, useCase, quality):
        self.sourceSystem = sourceSystem
        self.interfaceID = interfaceID
        self.databaseName = databaseName
        self.fileFormat = fileFormat
        self.useCase = useCase
        self.quality = quality
        self.landing_account = spark.conf.get("landing_account")
        self.env = spark.conf.get("env")
        self.loc = spark.conf.get("loc")
        self.instance = spark.conf.get("instance")
        self.dbScope = f"kv-sa-lnd-{self.env}-{self.loc}-{self.instance}"  # Key Vault Secret Scope

    def encrypt_val(self, clear_text, MASTER_KEY):
        if clear_text:
            cipher = AES.new(bytes(MASTER_KEY, 'ascii'), AES.MODE_CBC)
            cipher_text = cipher.encrypt(pad(clear_text.encode('UTF-8'), AES.block_size))
            textBase64 = base64.b64encode(cipher_text)
            return textBase64.decode('UTF-8')
        return clear_text

    def read_stream_data(self, landing_location, checkPointReadPath, csv_options):
        schema_location = f"{self.landing_account}/{landing_location}"
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("multiLine", "True")
            .option("cloudFiles.inferColumnTypes", "True")
            .option("recursiveFileLookup", "True")
            .option("cloudFiles.schemaLocation", schema_location)
            .options(**csv_options)
            .load(f"{self.landing_account}/{landing_location}")
        )
       
    def rename_invalid_columns(self, df):
        invalid_characters = [' ', '.', ',', ';', ':', '!', '@']
        for col in df.columns:
            new_col = col
            for char in invalid_characters:
                new_col = new_col.replace(char, '_')
            df = df.withColumnRenamed(col, new_col)
        return df

    def add_metadata(self, df):
        return df.select('*',
                  F.expr("uuid()").alias("md_process_id"),
                  F.expr("_metadata.file_path").alias("md_source_path"),
                  F.expr("_metadata.file_modification_time").alias("md_source_ts"),
                  F.expr("current_timestamp()").alias("md_created_ts")
                  )

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
        if self.is_valid_pii_category(config['pii_category']) and len(config['pii_columns']) > 0:
            for columnName in config['pii_columns']:
                if columnName in df.columns:
                    df = df.withColumn(columnName, F.expr(f"Python:DLTTableCreator().encrypt_val({columnName},'{encryptionKey}')"))
                else:
                    print(f"Warning: Column {columnName} not found in DataFrame")
        return df
    
    def create_live_tables(self, config):
        entity_name  = config['table_name']
        entity_name_safe = entity_name.replace(' ', '_')
        landing_location = config['dataPath']
        checkPointReadPath = config['checkPointReadPath']
        csv_options = config['csv_options']
        write_mode = config['write_mode'].lower()  # Support case-insensitive write modes

        @dlt.table(
            name=entity_name_safe,
            comment=entity_name,
            table_properties={
                "quality": self.quality,
                "pipeline.reset.allowed": "true",
                "useCase": self.useCase,
            }
        )
        def table_generator():
            df = self.read_stream_data(landing_location, checkPointReadPath, csv_options)
            df = self.rename_invalid_columns(df)
            df = self.add_metadata(df)
            if self.is_valid_pii_category(config['pii_category']):
                encryptionKey = dbutils.secrets.get(self.dbScope, config['pii_category'])
                df = self.enforce_pii(df, config, encryptionKey)
            
            if write_mode == 'append':
                return df  # Delta Live Tables will append by default
            elif write_mode == 'overwrite':
                return df  # In DLT, 'overwrite' would typically mean reprocessing, but here it appends data; explicit overwrite logic isn't handled as in traditional DataFrames.
            else:
                raise ValueError(f"Unsupported write mode: {write_mode}")

if __name__ == "__main__":
    sourceSystem = 'sourceSystem'
    interfaceID = 'interfaceID'
    databaseName = 'databaseName'
    fileFormat = 'delimited'
    useCase = 'useCase-1234/5678'
    quality = 'bronze'
    
    dlt_creator = DLTTableCreator(sourceSystem, interfaceID, databaseName, fileFormat, useCase, quality)
    
    table_config = [
        {
            'databaseName': 'production',
            'table_name': 'table1',
            'file_format': 'delimited',
            'dataPath': '/folder1/folder2/folder3/*/*/*/table1.csv',
            'checkPointReadPath': 'checkpointReadPath/table1',
            'flattern': True,
            'schema': None,
            'pii_category': 'colleague-secret',
            'pii_columns': ['col1', 'col2', 'col3'],
            'csv_options': {
                'delimiter': ',',
                'multiLine': True
            },
            'write_mode': 'append'  # Choose between 'append' and 'overwrite'
        }
    ]
    
    for config in table_config:
        dlt_creator.create_live_tables(config)
