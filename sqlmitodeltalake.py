import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, BooleanType, LongType, DecimalType, ArrayType
from concurrent.futures import ThreadPoolExecutor

class DataMigration:

    def __init__(self):
        self.spark = SparkSession.builder.appName("DataMigration").getOrCreate()
       
        self.env = os.environ['ENV']
        self.location = os.environ['LOCATION']
        self.key_vault_scope = f"kv-{self.env}-{self.location}"
       
        # Delta Lake Variables
        self.delta_account_name = f"deltalake{self.env}{self.location}"
        self.delta_account_key = dbutils.secrets.get(scope=self.key_vault_scope, key=f"key-delta{self.env}{self.location}")
        self.delta_container_name = "container_name"
       
        # Azure SQL MI Variables
        self.jdbc_hostname = dbutils.secrets.get(scope=self.key_vault_scope, key=f"jdbc-hostname-{self.env}-{self.location}")
        self.jdbc_port = dbutils.secrets.get(scope=self.key_vault_scope, key=f"jdbc-port-{self.env}-{self.location}")
        self.jdbc_database = dbutils.secrets.get(scope=self.key_vault_scope, key=f"jdbc-database-{self.env}-{self.location}")
        self.jdbc_url = f"jdbc:sqlserver://{self.jdbc_hostname}:{self.jdbc_port};database={self.jdbc_database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
        self.jdbc_username = dbutils.secrets.get(scope=self.key_vault_scope, key=f"jdbc-username-{self.env}-{self.location}")
        self.jdbc_password = dbutils.secrets.get(scope=self.key_vault_scope, key=f"jdbc-password-{self.env}-{self.location}")
        self.jdbc_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        
        self.set_storage_account()
        
    def set_storage_account(self):
        self.delta_storage_url = f"abfss://{self.delta_container_name}@{self.delta_account_name}.dfs.core.windows.net"
        self.spark.conf.set(f"fs.azure.account.key.{self.delta_account_name}.dfs.core.windows.net", self.delta_account_key)
           
    # Read full data from Azure SQL MI table
    def read_data_from_sqlmi(self, table_name):
        df = self.spark.read.format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", self.jdbc_username) \
            .option("password", self.jdbc_password) \
            .option("driver", self.jdbc_driver) \
            .load()
        return df
    
    # Get max value of a column from Delta Lake table
    def get_max_value(self, table_name, column_name):
        try:
            last_max_value = self.spark.read.format("delta").table(table_name).select(max(column_name)).collect()[0][0]
        except AnalysisException:
            last_max_value = None
        return last_max_value
           
    # Read incremental data from Azure SQL MI table based on max value of a column
    def read_incremental_data_from_sqlmi(self, schema_name, table_name, incremental_column, last_max_value):
        dbtable = f"{schema_name}.{table_name}"
        query = f"(select * from {dbtable} where {incremental_column} > '{last_max_value}') as query"
        df = self.spark.read.format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", query) \
            .option("user", self.jdbc_username) \
            .option("password", self.jdbc_password) \
            .option("driver", self.jdbc_driver) \
            .load()
        return df
    
    # Read data in partitions from Azure SQL MI table
    def read_full_data_in_partitions(self, schema_name, table_name, partition_column, num_partitions):
        dbtable = f"{schema_name}.{table_name}"
    
        # Query to get distinct partition values
        query = f"(SELECT DISTINCT {partition_column} FROM {dbtable}) as query"
        bounds_df = self.spark.read \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", query) \
            .option("user", self.jdbc_username) \
            .option("password", self.jdbc_password) \
            .option("driver", self.jdbc_driver) \
            .load()
    
        # Extract distinct partition values
        partition_values = [row[partition_column] for row in bounds_df.collect()]
        partition_values.sort()
    
        # Initialize list to store DataFrames
        partition_dfs = []
    
        # Function to fetch data for a partition
        def fetch_partition_data(partition_value):
            partition_query = f"(SELECT * FROM {dbtable} WHERE {partition_column} = '{partition_value}') as query"
            partition_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", partition_query) \
                .option("user", self.jdbc_username) \
                .option("password", self.jdbc_password) \
                .option("driver", self.jdbc_driver) \
                .load()
            return (partition_value, partition_df)
    
        # Use ThreadPoolExecutor to process partitions in parallel
        with ThreadPoolExecutor(max_workers=num_partitions) as executor:
            futures = [executor.submit(fetch_partition_data, pv) for pv in partition_values]
            for future in futures:
                partition_dfs.append(future.result())
    
        return partition_dfs

    # Function to write partition data
    def write_partition(self, partition):
        partition_value, partition_df = partition
        delta_path = f"{self.delta_storage_url}/partition={partition_value}"
        partition_df.write.format("delta").mode("overwrite").save(delta_path)

    # Write data to Delta Lake with overwrite mode
    def write_overwrite(self, df, delta_path, partition_column=None):
        write_mode = "overwrite"
        try:
            if partition_column:
                df.write.format("delta").mode(write_mode).partitionBy(partition_column).save(delta_path)
            else:
                df.write.format("delta").mode(write_mode).save(delta_path)
        except AnalysisException as e:
            print(f"Error writing data to {delta_path}: {e}")

    # Write overwrite mode by partition overwrite mode dynamic as option
    def write_overwrite_by_partition(self, partition_dfs, delta_path):
        write_mode = "overwrite"
        try:
            with ThreadPoolExecutor(max_workers=len(partition_dfs)) as executor:
                futures = [executor.submit(self.write_partition, partition) for partition in partition_dfs]
                for future in futures:
                    future.result()
        except AnalysisException as e:
            print(f"Error writing data to {delta_path}: {e}")
    
    # Process full table
    def process_full_table(self, schema_name, table_name, delta_path, partition_column=None):
        df = self.read_data_from_sqlmi(f"{schema_name}.{table_name}")
        self.write_overwrite(df, delta_path, partition_column)
        print(f"Data written to {delta_path}")
    
    # Process incremental table
    def process_incremental_table(self, schema_name, table_name, delta_path, partition_column=None, incremental_column=None):
        last_max_value = self.get_max_value(delta_path, incremental_column)
        if last_max_value is None:
            partition_dfs = self.read_full_data_in_partitions(schema_name, table_name, partition_column, num_partitions=700)  # Adjust num_partitions as needed
            self.write_overwrite_by_partition(partition_dfs, delta_path)
        else:
            df = self.read_incremental_data_from_sqlmi(schema_name, table_name, incremental_column, last_max_value)
            self.write_overwrite_by_partition([(None, df)], delta_path)
        print(f"Incremental data written to {delta_path}")

if __name__ == "__main__":
    data_migration = DataMigration()
    
    # Table 1 for full load small dimension table
    schema_name = "dbo"
    table_name = "dim_table"
    delta_path = f"{data_migration.delta_storage_url}/dim_table"
    data_migration.process_full_table(schema_name, table_name, delta_path)
    
    # Table 2 for incremental load large fact table
    schema_name = "dbo"
    table_name = "fact_table"
    delta_path = f"{data_migration.delta_storage_url}/fact_table"
    partition_column = "datekey"
    incremental_column = "last_updated"
    data_migration.process_incremental_table(schema_name, table_name, delta_path, partition_column, incremental_column)
