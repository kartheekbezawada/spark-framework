import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import sha2, concat_ws
# Other imports remain the same

class DatabricksConnector:
    # Existing methods...

    def get_checksum(self, df):
        """
        Calculate a checksum for the DataFrame.
        """
        checksum_df = df.withColumn("row_hash", sha2(concat_ws("||", *df.columns), 256))
        total_hash = checksum_df.agg(sha2(concat_ws("||", *checksum_df.columns), 256)).collect()[0][0]
        return total_hash

    def verify_data_integrity(self, source_df, dest_table_name):
        """
        Perform data integrity checks: row count and checksum verification.
        """
        source_row_count = source_df.count()
        source_checksum = self.get_checksum(source_df)

        # Row count verification
        dest_row_count = self.get_row_count_from_sql(dest_table_name)
        if source_row_count != dest_row_count:
            return False, "Row count mismatch"

        # Checksum verification
        dest_df = self.read_from_sql_server(dest_table_name)
        dest_checksum = self.get_checksum(dest_df)
        if source_checksum != dest_checksum:
            return False, "Checksum mismatch"

        return True, "Data integrity verified"

    def get_row_count_from_sql(self, table_name):
        """
        Get the row count from the SQL Server table.
        """
        jdbc_url = self._get_jdbc_url()
        query = f"(SELECT COUNT(*) as cnt FROM {table_name}) as count_table"
        count_df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", query) \
            .option("user", self.jdbc_username) \
            .option("password", self.jdbc_password) \
            .load()
        return count_df.collect()[0]["cnt"]

    def read_from_sql_server(self, table_name):
        """
        Read data from SQL Server table.
        """
        jdbc_url = self._get_jdbc_url()
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", self.jdbc_username) \
            .option("password", self.jdbc_password) \
            .load()
        return df

    # Modify the process_table method to include data integrity check
    def process_table(self, table_name):
        blob_path = f"{self.alpha_storage_url}/{table_name}"
        source_df = self.read_from_blob_storage(blob_path)
        if source_df is not None and not source_df.rdd.isEmpty():
            self.write_to_sql_server(source_df, table_name=table_name)
            
            # Data Integrity Check
            integrity_ok, message = self.verify_data_integrity(source_df, table_name)
            if not integrity_ok:
                print(f"Data integrity check failed for table {table_name}: {message}")
                # Handle the integrity check failure (e.g., log the error, retry, etc.)
                return

            self.migration_log_info(table_name, blob_path)
            # Delete the folder after successful migration
            self.delete_folder_from_blob_storage(table_name)
        else:
            print(f"No data found in blob path: {blob_path}")

    # Rest of the existing code...

if __name__ == "__main__":
    # Main execution code...
