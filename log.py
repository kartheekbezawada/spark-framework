from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import datetime

class DataProcessor:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.log_table = "your_log_table"  # Specify your log table name or path

    def log(self, message, level="INFO"):
        """
        Logs a message with the given level to the log table.

        Parameters:
        - message: The log message.
        - level: The log level (INFO, WARNING, ERROR).
        """
        current_time = datetime.datetime.now()
        log_df = self.spark.createDataFrame([(current_time, level, message)], ["timestamp", "level", "message"])
        log_df.write.format("delta").mode("append").saveAsTable(self.log_table)  # Adjust format and saving method as needed

    def validate_dataframe(self, df, expected_row_count=None, expected_columns=None):
        """
        Validates a DataFrame against expected row counts and column names.

        Parameters:
        - df: The DataFrame to validate.
        - expected_row_count: The expected number of rows.
        - expected_columns: A list of expected column names.
        """
        if expected_row_count is not None:
            actual_row_count = df.count()
            if actual_row_count != expected_row_count:
                self.log(f"Row count mismatch. Expected: {expected_row_count}, Got: {actual_row_count}", "ERROR")
                return False

        if expected_columns is not None:
            actual_columns = df.columns
            for column in expected_columns:
                if column not in actual_columns:
                    self.log(f"Missing column: {column}", "ERROR")
                    return False

        self.log("DataFrame validation passed.")
        return True

    def process_data(self, df_large, df_small):
        """
        Example data processing function that joins two DataFrames and performs a calculation.

        Parameters:
        - df_large: A large DataFrame.
        - df_small: A smaller DataFrame to be broadcast.
        """
        try:
            # Example validation
            if not self.validate_dataframe(df_large, expected_columns=["col1", "col2"]) or \
               not self.validate_dataframe(df_small, expected_columns=["col1", "col3"]):
                raise ValueError("Data validation failed.")

            # Example processing: Joining DataFrames
            result_df = df_large.join(df_small.hint("broadcast"), "col1")

            self.log("Data processing completed successfully.")
            return result_df
        except Exception as e:
            self.log(f"Data processing failed: {str(e)}", "ERROR")
            return None

# Example usage
if __name__ == "__main__":
    spark = SparkSession.builder.appName("Data Validation and Processing").getOrCreate()

    # Initialize the DataProcessor object
    processor = DataProcessor(spark)

    # Example: Reading data
    df_large = spark.read.format("delta").load("path_to_large_table")
    df_small = spark.read.format("delta").load("path_to_small_table")

    # Process data
    result = processor.process_data(df_large, df_small)

    # Assuming result is not None, you might want to save or further process the result DataFrame
    if result:
        result.show()
