from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, current_date, date_sub, substring, when, concat_ws, year, month, sum
from pyspark.sql.window import Window
from delta.tables import DeltaTable

class PayrollDataProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.alpha_account_name = "storage_account_name"
        self.alpha_account_key = "storage_account_key"
        self.alpha_container_name = "storage_container_name"
        self.alpha_storage_config = {"fs.azure.account.key." + self.alpha_account_name + ".blob.core.windows.net": self.alpha_account_key}
        self.alpha_storage_url = f"wasbs://{self.alpha_container_name}@{self.alpha_account_name}.blob.core.windows.net"
        self.set_storage_configuration()  # Set Azure Blob Storage configuration

    def set_storage_configuration(self):
        # Sets the Azure Blob Storage configuration on the Spark session
        self.spark.conf.set(f"fs.azure.account.key.{self.alpha_account_name}.blob.core.windows.net", self.alpha_account_key)

    def read_delta_table(self, path):
        # Reads a Delta table from the specified path
        table_path = f"{self.alpha_storage_url}/{path}"
        return self.spark.read.format("delta").load(table_path)

    def write_delta_table(self, df, delta_table_path):
        # Writes (merges) the DataFrame into the specified Delta table path
        full_delta_table_path = f"{self.alpha_storage_url}/{delta_table_path}"
        if DeltaTable.isDeltaTable(self.spark, full_delta_table_path):
            deltaTable = DeltaTable.forPath(self.spark, full_delta_table_path)
            deltaTable.alias("target").merge(
                df.alias("source"),
                "target.composite_key = source.composite_key"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            df.write.format("delta").mode("append").partitionBy("year", "month").save(full_delta_table_path)

    # Your additional methods like process_colleague_rates, process_colleague_base_rate, etc., remain unchanged

    def aggregate_transformed_data(self, df):
        # Aggregates the transformed DataFrame
        aggregated_df = df.groupBy("cwh_store_number", "cwh_division_number", "cwh_datekey") \
            .agg(
                sum("cwh_wrks_hrs").alias("sum_wrkd_hrs"),
                sum("calculated_wages").alias("sum_calculated_wages")
            )
        return aggregated_df
if __name__ == "__main__":
    spark = SparkSession.builder.appName("PayrollDataProcessorApp") \
        .getOrCreate()

    processor = PayrollDataProcessor(spark)

    # Example usage of reading a Delta table
    delta_table_path = "path/to/your/delta/table"
    df = processor.read_delta_table(delta_table_path)

    # Process your DataFrame as needed, then write or merge back into Delta
    # Assume df_transformed is your processed DataFrame
    # df_transformed = processor.some_processing_method(df)

    # Write or merge the processed DataFrame into a Delta table
    processor.write_delta_table(df_transformed, "processed_delta_table")

    # Aggregate transformed data
    aggregated_df = processor.aggregate_transformed_data(df_transformed)
    aggregated_df.show()
