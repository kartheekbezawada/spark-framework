from pyspark.sql import SparkSession

class Configuration:
    def __init__(self):
        self.account_name = "storage_account_name"
        self.account_key = "storage_account_key"
        self.container_name = "storage_container_name"
        self.storage_config = {
            "fs.azure.account.key." + self.account_name + ".blob.core.windows.net": self.account_key
        }
        self.storage_url = f"wasbs://{self.container_name}@{self.account_name}.blob.core.windows.net"

class ReadDelta:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def read_table(self, path):
        table_path = f"{self.config.storage_url}/{path}"
        return self.spark.read.format("delta").options(**self.config.storage_config).load(table_path)

class ProcessData:
    @staticmethod
    def prefix_columns(df, prefix):
        for col_name in df.columns:
            df = df.withColumnRenamed(col_name, f"{prefix}_{col_name}")
        return df

    @staticmethod
    def filter_data(df, condition):
        return df.filter(condition)

class TransformData:
    def __init__(self, spark):
        self.spark = spark

    def create_temp_views(self, dataframes):
        for name, df in dataframes.items():
            df.createOrReplaceTempView(name)

    def run_transformation_query(self, query):
        return self.spark.sql(query)

class WriteDelta:
    def __init__(self, config):
        self.config = config

    def write_table(self, df, path):
        table_path = f"{self.config.storage_url}/{path}"
        df.write.format("delta").mode(mode).options(**self.config.storage_config).save(table_path)

    def merge_or_create_delta_table(self, source_df, target_path, condition, merge=True): 
        target_table_path = f"{self.config['storage_url']}/{target_path}"
        if DeltaTable.isDeltaTable(self.spark, target_table_path):
            if merge:
                # Load the target table as a DeltaTable
                deltaTable = DeltaTable.forPath(self.spark, target_table_path)
                # Perform the merge operation
                (deltaTable.alias("target")
                 .merge(
                     source_df.alias("source"),
                     condition)
                 .whenMatchedUpdateAll()  
                 .whenNotMatchedInsertAll()  
                 .execute()
                )
        else:
            # If the target Delta table does not exist, create it from the source DataFrame
            print(f"Creating new Delta table at {target_table_path}")
            source_df.write.format("delta").mode("overwrite").save(target_table_path)
    
