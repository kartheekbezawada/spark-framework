from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, randn, lit, current_date
import random
import string

# Initialize Spark Session
spark = SparkSession.builder.appName("SimpleDataGenerator").getOrCreate()

# set defualt log level to info
spark.sparkContext.setLogLevel("INFO")
# Parameters for DataFrame
num_rows = 1000  # Number of rows in the DataFrame
num_columns = 5  # Number of columns

# Create a DataFrame
df = spark.range(num_rows)

# Add columns with different data types
for col_idx in range(num_columns):
    column_type = random.choice(['int', 'double', 'string', 'date'])
    if column_type == 'int':
        df = df.withColumn(f"int_col_{col_idx}", (rand() * 100).cast('integer'))
    elif column_type == 'double':
        df = df.withColumn(f"double_col_{col_idx}", randn())
    elif column_type == 'string':
        random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        df = df.withColumn(f"string_col_{col_idx}", lit(random_string))
    elif column_type == 'date':
        df = df.withColumn(f"date_col_{col_idx}", current_date())

# Show the DataFrame
df.show()

# Write the DataFrame to a parquet file in local folder in perssonal computer
df.write.parquet("C:/Users/Aryan/OneDrive/Desktop/Migration")



# Stop the Spark session
spark.stop()




 