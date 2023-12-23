from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, randn, lit, current_date, sha2, col
import random
import string

class SyntheticDataGenerator:
    def __init__(self, storage_account_name, storage_account_access_key, output_container):
        self.spark = SparkSession.builder.appName("SyntheticDataGenerator").getOrCreate()
        self.storage_account_name = storage_account_name
        self.storage_account_access_key = storage_account_access_key
        self.output_container = output_container
        self.configure_spark()

    def configure_spark(self):
        # Configure Spark to access Azure Blob Storage
        self.spark.conf.set(
            f"fs.azure.account.key.{self.storage_account_name}.blob.core.windows.net", 
            self.storage_account_access_key
        )

    def generate_data(self, num_folders, num_files_per_folder, num_columns, num_rows_per_file):
        for folder_idx in range(num_folders):
            folder_path = f"{self.output_container}/folder_{folder_idx}"
            for file_idx in range(num_files_per_folder):
                df = self.create_dataframe(num_columns, num_rows_per_file)
                df_hashed = self.create_hashed_dataframe(df)
                self.write_parquet(df_hashed, folder_path, file_idx)

    def create_dataframe(self, num_columns, num_rows):
        df = self.spark.range(num_rows)
        for col_idx in range(num_columns):
            column_type = random.choice(['int', 'long', 'double', 'string', 'varchar', 'date', 'boolean'])
            if column_type == 'int':
                df = df.withColumn(f"int_col_{col_idx}", (rand() * 100).cast('integer'))
            elif column_type == 'long':
                df = df.withColumn(f"long_col_{col_idx}", (rand() * 100000).cast('long'))
            elif column_type == 'double':
                df = df.withColumn(f"double_col_{col_idx}", randn())
            elif column_type in ['string', 'varchar']:
                df = df.withColumn(f"string_col_{col_idx}", lit(''.join(random.choices(string.ascii_letters + string.digits, k=10))))
            elif column_type == 'date':
                df = df.withColumn(f"date_col_{col_idx}", current_date())
            elif column_type == 'boolean':
                df = df.withColumn(f"boolean_col_{col_idx}", (rand() > 0.5))
        return df

    def create_hashed_dataframe(self, df, hash_bits=256):
        df_hashed = df
        for column in df.columns:
            df_hashed = df_hashed.withColumn(column, sha2(col(column), hash_bits))
        return df_hashed

    def write_parquet(self, df, folder_path, file_idx):
        file_path = f"{folder_path}/file_{file_idx}.parquet"
        df.write.mode("overwrite").parquet(f"wasbs://{file_path}")

    def stop_spark(self):
        self.spark.stop()


# Usage example:
storage_account_name = "your-storage-account-name"  # Replace with your Azure Storage account
storage_account_access_key = "your-access-key"  # Replace with your Azure Storage account access key
output_container = "your-container-name"  # Replace with your Azure Blob Storage container name

generator = SyntheticDataGenerator(storage_account_name, storage_account_access_key, output_container)

# Generate and store hashed data
num_folders = 10
num_files_per_folder = 5
num_columns = 10
num_rows_per_file = 1000000  # 1 million rows per file
generator.generate_data(num_folders, num_files_per_folder, num_columns, num_rows_per_file)

# Stop Spark session when done
generator.stop_spark()
