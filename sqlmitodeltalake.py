import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max
from pyspark.sql.types import *
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

    def _read_incremental_data(self, schema_name, table_name, incremental_column, delta_path, partition_column, num_partitions):
        try:
            last_max_value = self.spark.read.format("delta").load(delta_path) \
                .select(max(col(incremental_column)).alias("last_max_value")) \
                .collect()[0]["last_max_value"]
        except AnalysisException:
            last_max_value = None

        if last_max_value is not None:
            dbtable = f"{schema_name}.{table_name}"
            query = f"(SELECT * FROM {dbtable} WHERE {incremental_column} > '{last_max_value}') AS src"
            return self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", query) \
                .option("user", self.jdbc_username) \
                .option("password", self.jdbc_password) \
                .load()
        else:
            # No delta table or empty delta table, read full data
            query = f"(SELECT MIN({partition_column}) AS min_value, MAX({partition_column}) AS max_value FROM {schema_name}.{table_name}) AS bounds"
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

    def _map_data_types(self, df):
        type_mapping = {
            'int': IntegerType(),
            'bigint': LongType(),
            'smallint': ShortType(),
            'tinyint': ByteType(),
            'bit': BooleanType(),
            'float': FloatType(),
            'real': FloatType(),
            'decimal': DecimalType(),
            'numeric': DecimalType(),
            'smallmoney': DecimalType(),
            'money': DecimalType(),
            'datetime': TimestampType(),
            'datetime2': TimestampType(),
            'smalldatetime': TimestampType(),
            'date': DateType(),
            'time': StringType(),
            'char': StringType(),
            'varchar': StringType(),
            'text': StringType(),
            'nchar': StringType(),
            'nvarchar': StringType(),
            'ntext': StringType(),
            'binary': BinaryType(),
            'varbinary': BinaryType(),
            'image': BinaryType()
        }

        for field in df.schema.fields:
            sql_type = field.dataType.simpleString()
            if sql_type in type_mapping:
                df = df.withColumn(field.name, col(field.name).cast(type_mapping[sql_type]))

        return df

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

    def _write_merge(self, partition_df, delta_path):
        delta_table = DeltaTable.forPath(self.spark, delta_path)
        merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in partition_df.columns if col in delta_table.toDF().columns])
        delta_table.alias("target").merge(
            partition_df.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    def _write_partition_overwrite(self, partition_df, delta_path):
        partition_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", "true") \
            .save(delta_path)

    def _write_partition(self, partition, delta_path, write_mode):
        try:
            partition_df = self.spark.createDataFrame(partition)
            partition_df = self._map_data_types(partition_df)
            if write_mode == "overwrite":
                self._write_overwrite(partition_df, delta_path)
            elif write_mode == "append":
                self._write_append(partition_df, delta_path)
            elif write_mode == "merge":
                self._write_merge(partition_df, delta_path)
            elif write_mode == "partition_overwrite":
                self._write_partition_overwrite(partition_df, delta_path)
            else:
                raise ValueError(f"Unsupported write mode: {write_mode}")
        except AnalysisException as e:
            print(f"Error in writing partition: {e}")

    def _process_table(self, schema_name, table_name, partition_column, num_partitions, write_mode, incremental_column=None):
        delta_path = f"abfss://{self.key_vault_scope}@datalake.dfs.core.windows.net/{table_name}"
        
        if write_mode in ["overwrite", "append"]:
            # Full load
            df = self._read_full_data(schema_name, table_name)
            # Convert data types and write each partition individually
            df.foreachPartition(lambda partition: self._write_partition(partition, delta_path, write_mode))

        elif write_mode == "merge" and incremental_column:
            # Incremental load
            df = self._read_incremental_data(schema_name, table_name, incremental_column, delta_path, partition_column, num_partitions)
            # Convert data types and write each partition individually
            df.foreachPartition(lambda partition: self._write_partition(partition, delta_path, write_mode))
        
        elif write_mode == "partition_overwrite":
            # Partition overwrite load
            query = f"(SELECT MIN({partition_column}) AS min_value, MAX({partition_column}) AS max_value FROM {schema_name}.{table_name}) AS bounds"
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

            df = self._read_data_in_partitions(schema_name, table_name, partition_column, lower_bound, upper_bound, num_partitions)
            # Convert data types and write each partition individually
            df.foreachPartition(lambda partition: self._write_partition(partition, delta_path, write_mode))

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
        {"schema_name": "schema2", "table_name": "table3", "partition_column": "id", "num_partitions": 256, "write_mode": "merge", "incremental_column": "timestamp_column"},
        {"schema_name": "schema2", "table_name": "table4", "partition_column": "id", "num_partitions": 256, "write_mode": "partition_overwrite"},
        {"schema_name": "schema2", "table_name": "table5", "partition_column": "id", "num_partitions": 256, "write_mode": "merge", "incremental_column": "timestamp_column"}
    ]

    data_migration.run(tables)
