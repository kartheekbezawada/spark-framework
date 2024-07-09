import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

class DataMigration:
    def __init__(self, key_vault_scope):
        self.key_vault_scope = key_vault_scope
        self.spark = self._initialize_spark()
        self._load_secrets()

    def _initialize_spark(self):
        return SparkSession.builder \
            .appName("Parallel Read from SQL MI and Write to Delta Lake") \
            .getOrCreate()

    def _load_secrets(self):
        self.jdbc_username = dbutils.secrets.get(scope=self.key_vault_scope, key="jdbc_username")
        self.jdbc_password = dbutils.secrets.get(scope=self.key_vault_scope, key="jdbc_password")
        self.jdbc_url = dbutils.secrets.get(scope=self.key_vault_scope, key="jdbc_url")

    def _read_full_data(self, schema_name, table_name):
        dbtable = f"{schema_name}.{table_name}"
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", dbtable) \
            .option("user", self.jdbc_username) \
            .option("password", self.jdbc_password) \
            .load()

    def _read_data_in_partitions(self, schema_name, table_name, partition_column, lower_bound, upper_bound, num_partitions):
        dbtable = f"{schema_name}.{table_name}"
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", dbtable) \
            .option("user", self.jdbc_username) \
            .option("password", self.jdbc_password) \
            .option("partitionColumn", partition_column) \
            .option("lowerBound", lower_bound) \
            .option("upperBound", upper_bound) \
            .option("numPartitions", num_partitions) \
            .load()

    def _get_last_max_value(self, delta_path, incremental_column):
        try:
            last_max_value = self.spark.read.format("delta").load(delta_path) \
                .select(max(col(incremental_column)).alias("last_max_value")) \
                .collect()[0]["last_max_value"]
        except AnalysisException:
            last_max_value = None
        return last_max_value

    def _read_incremental_full_table(self, schema_name, table_name, incremental_column, last_max_value):
        dbtable = f"{schema_name}.{table_name}"
        query = f"(SELECT * FROM {dbtable} WHERE {incremental_column} > '{last_max_value}') AS src"
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", query) \
            .option("user", self.jdbc_username) \
            .option("password", self.jdbc_password) \
            .load()
    
    def _read_full_data_in_partitions(self, schema_name, table_name, partition_column, num_partitions):
        dbtable = f"{schema_name}.{table_name}"
        query = f"(SELECT MIN({partition_column}) AS min_value, MAX({partition_column}) AS max_value FROM {dbtable}) AS bounds"
        bounds_df = self.spark.read \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", query) \
            .option("user", self.jdbc_username) \
            .option("password", self.jdbc_password) \
            .load()

        bounds = bounds_df.collect()[0]
        lower_bound = bounds["min_value"]
        upper_bound = bounds["max_value"]

        return self._read_data_in_partitions(schema_name, table_name, partition_column, lower_bound, upper_bound, num_partitions)

    def _write_overwrite(self, partition_df, delta_path):
        partition_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(delta_path)

    def _write_append(self, partition_df, delta_path):
        partition_df.write \
            .format("delta") \
            .mode("append") \
            .save(delta_path)

    def _write_partition(self, partition, delta_path, write_mode):
        try:
            partition_df = self.spark.createDataFrame(partition)
            if write_mode == "overwrite":
                self._write_overwrite(partition_df, delta_path)
            elif write_mode == "append":
                self._write_append(partition_df, delta_path)
            else:
                raise ValueError(f"Unsupported write mode: {write_mode}")
        except AnalysisException as e:
            print(f"Error in writing partition: {e}")

    def _process_incremental_table(self, schema_name, table_name, partition_column, num_partitions, incremental_column, delta_path):
        last_max_value = self._get_last_max_value(delta_path, incremental_column)
        if last_max_value is None:
            df = self._read_full_data_in_partitions(schema_name, table_name, partition_column, num_partitions)
        else:
            df = self._read_incremental_full_table(schema_name, table_name, incremental_column, last_max_value)
        # Write each partition individually
        df.foreachPartition(lambda partition: self._write_partition(partition, delta_path, "append"))

    def _process_table(self, schema_name, table_name, partition_column, num_partitions, write_mode, incremental_column=None):
        delta_path = f"abfss://{self.key_vault_scope}@datalake.dfs.core.windows.net/{table_name}"
        
        if write_mode == "overwrite":
            full_df = self._read_full_data(schema_name, table_name)
            self._write_overwrite(full_df, delta_path)
        
        elif write_mode == "append":
            self._process_incremental_table(schema_name, table_name, partition_column, num_partitions, incremental_column, delta_path)

    def run(self, tables):
        for table in tables:
            schema_name = table["schema_name"]
            table_name = table["table_name"]
            partition_column = table.get("partition_column")
            num_partitions = table.get("num_partitions", 1)
            write_mode = table["write_mode"]
            incremental_column = table.get("incremental_column")
            self._process_table(schema_name, table_name, partition_column, num_partitions, write_mode, incremental_column)


if __name__ == "__main__":
    key_vault_scope = "my_scope"
    data_migration = DataMigration(key_vault_scope)

    tables = [
        {"schema_name": "schema1", "table_name": "table1", "partition_column": "id", "num_partitions": 256, "write_mode": "overwrite"},
        {"schema_name": "schema1", "table_name": "table2", "partition_column": "id", "num_partitions": 256, "write_mode": "append"},
        {"schema_name": "schema2", "table_name": "table3", "partition_column": "id", "num_partitions": 256, "write_mode": "append", "incremental_column": "timestamp_column"},
        {"schema_name": "schema2", "table_name": "table4", "partition_column": "id", "num_partitions": 256, "write_mode": "overwrite"},
        {"schema_name": "schema2", "table_name": "table5", "partition_column": "id", "num_partitions": 256, "write_mode": "append", "incremental_column": "timestamp_column"}
    ]

    data_migration.run(tables)
