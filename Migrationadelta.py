from pyspark.sql import SparkSession

class Validation:
    @staticmethod
    def validate_schema(source_df, dest_df):
        if source_df.schema != dest_df.schema:
            print(f"Schema Validation failed: Source({source_df.schema}) vs Destination({dest_df.schema})")
        # else scehema validation pass for table with table name    
        else:
            prnt(f"Schema Validation passed for table {source_df}")

    @staticmethod
    def validate_data_types(source_df, dest_df):
        for source_field, dest_field in zip(source_df.schema.fields, dest_df.schema.fields):
            if source_field.dataType != dest_field.dataType:
                print(f"Data type mismatch for column '{source_field.name}': Source({source_field.dataType}) vs Destination({dest_field.dataType})")
            else:
                print(f"Data type matched for column '{source_field.name}': Source({source_field.dataType}) vs Destination({dest_field.dataType})") 

    @staticmethod
    def validate_row_count(source_df, dest_df):
        source_count = source_df.count()
        dest_count = dest_df.count()
        if source_count != dest_count:
            print(f"Row count mismatch: Source({source_count}) vs Destination({dest_count})")
        else:   
            print(f"Row count matched: Source({source_count}) vs Destination({dest_count})")

    @staticmethod
    def validate_partition_count(source_df, dest_df):
        source_partitions = source_df.rdd.getNumPartitions()
        dest_partitions = dest_df.rdd.getNumPartitions()
        if source_partitions != dest_partitions:
            print(f"Partition count mismatch: Source({source_partitions}) vs Destination({dest_partitions})")
        else:
            print(f"Partition count matched: Source({source_partitions}) vs Destination({dest_partitions})")
            
    @staticmethod
    def validate_nullable_properties(source_df, dest_df):
        for source_field, dest_field in zip(source_df.schema.fields, dest_df.schema.fields):
            if source_field.nullable != dest_field.nullable:
                print(f"Nullable property mismatch for column '{source_field.name}': Source({source_field.nullable}) vs Destination({dest_field.nullable})")
            else:
                print(f"Nullable property matched for column '{source_field.name}': Source({source_field.nullable}) vs Destination({dest_field.nullable})")
    

class DeltaTableMigrator(Validation):

    def __init__(self, spark, key_vault_scope):
        self.spark = spark
        self.key_vault_scope = key_vault_scope
        
        # Azure Data Lake Storage Gen2 credentials for alpha
        self.alpha_account_name = dbutils.secrets.get(scope=self.key_vault_scope, key="blob-storage-account-name")
        self.alpha_account_key = dbutils.secrets.get(scope=self.key_vault_scope, key="blob-storage-account-key")
        self.alpha_container_name = dbutils.secrets.get(scope=self.key_vault_scope, key="blob-container-name")
        self.alpha_storage_url = f"abfss://{self.alpha_container_name}@{self.alpha_account_name}.dfs.core.windows.net"
        self.alpha_storage_url_https = f"https://{self.alpha_account_name}.blob.core.windows.net"
        self.alpha_storage_config = {f"fs.azure.account.key.{self.alpha_account_name}.blob.core.windows.net": self.alpha_account_key}

        self.beta_account_name = dbutils.secrets.get(scope=self.key_vault_scope, key="blob-storage-account-name-beta")
        self.beta_account_key = dbutils.secrets.get(scope=self.key_vault_scope, key="blob-storage-account-key-beta")
        self.beta_container_name = dbutils.secrets.get(scope=self.key_vault_scope, key="blob-container-name-beta")
        self.beta_storage_url = f"abfss://{self.beta_container_name}@{self.beta_account_name}.dfs.core.windows.net"
        self.beta_storage_url_https = f"https://{self.beta_account_name}.blob.core.windows.net"
        self.beta_storage_config = {f"fs.azure.account.key.{self.beta_account_name}.blob.core.windows.net": self.beta_account_key}
        
        
    def read_delta_table(self, table_path):
        try:
            df = self.spark.read \
                        .format("delta") \
                        .options(**self.alpha_storage_config) \
                        .load(table_path)          
            return df
        except Exception as e:
            print(f"Error reading Delta table: {e}")
            return None
    
    def write_delta_table(self, df, table_path):
        try:
            df.write.format("delta").mode("overwrite").save(table_path)
            return True
        except Exception as e:
            print(f"Error writing Delta table: {e}")
            return False
        
    def move_delta_table(self, stage_table_path, prod_table_path):
        stage_df = self.spark.read.format("delta").load(stage_table_path)
        stage_df.write.format("delta").mode("overwrite").save(prod_table_path)
    
    def migrate_tables(self, tables_to_move):
        for table, paths in tables_to_move.items():
            print(f"Moving {table} from Stage to Prod...")
            self.move_delta_table(paths["stage_path"], paths["prod_path"])
        print("All tables migrated successfully!")
            
    # Do final validations and print the results, if any validation fails, print validation name nd say fail or pass
    def validate_migration(self, stage_df, prod_df):
        for table, paths in tables_to_move.items():
            print(f"Validating {table}...")
            self.validate_schema(stage_df, prod_df)
            self.validate_data_types(stage_df, prod_df)
            self.validate_row_count(stage_df, prod_df)
            self.validate_partition_count(stage_df, prod_df)
            self.validate_nullable_properties(stage_df, prod_df)
                    
   
# Example usage
spark = SparkSession.builder.appName("Delta Table Migration").getOrCreate()
key_vault_scope = "key_vault_scope_migration"

# dictionary of tables to move with their stage and prod paths
tables_to_move = {
    "table1": {
        "stage": "table1_stage",
        "prod": "table1_prod"
    },
    "table2": {
        "stage": "table2_stage",
        "prod": "table2_prod"
    }
}

migrator = DeltaTableMigrator(spark, key_vault_scope)
migrator.migrate_tables(tables_to_move)
