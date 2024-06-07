# Set up configurations for accessing Azure Data Lake Storage Gen2
spark.conf.set("fs.azure.account.key.<your-storage-account-name>.dfs.core.windows.net", "<your-storage-account-key>")

# Define the path to the XML file in your Data Lake
xml_file_path = "abfss://<your-container-name>@<your-storage-account-name>.dfs.core.windows.net/path/to/your/xmlfile.xml"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

# Initialize Spark session
spark = SparkSession.builder.appName("XML to DataFrame").getOrCreate()

# Load the XML file
df = spark.read.format("xml") \
    .option("rowTag", "wd:Report_Entry") \
    .load(xml_file_path)

# Display the schema to understand the structure
df.printSchema()

# Flatten the DataFrame by extracting nested elements
flattened_df = df.select(
    col("wd:Colleague_ID").alias("Colleague_ID"),
    col("wd:Pattern_ID").alias("Pattern_ID"),
    col("wd:Schedule_Calender.wd:Pattern_Start_Date").alias("Pattern_Start_Date"),
    explode(col("wd:Schedule_Pattern_Weeks")).alias("Schedule_Pattern_Weeks")
)

# Flatten the nested structure within Schedule_Pattern_Weeks
flattened_df = flattened_df.select(
    col("Colleague_ID"),
    col("Pattern_ID"),
    col("Pattern_Start_Date"),
    col("Schedule_Pattern_Weeks.wd:Week_Number._wd:Descriptor").alias("Week_Descriptor"),
    col("Schedule_Pattern_Weeks.wd:Start_Time_Day_1").alias("Start_Time_Day_1"),
    col("Schedule_Pattern_Weeks.wd:End_Time_Day_1").alias("End_Time_Day_1"),
    col("Schedule_Pattern_Weeks.wd:Start_Time_Day_2").alias("Start_Time_Day_2"),
    col("Schedule_Pattern_Weeks.wd:End_Time_Day_2").alias("End_Time_Day_2"),
    col("Schedule_Pattern_Weeks.wd:Start_Time_Day_3").alias("Start_Time_Day_3"),
    col("Schedule_Pattern_Weeks.wd:End_Time_Day_3").alias("End_Time_Day_3"),
    col("Schedule_Pattern_Weeks.wd:Start_Time_Day_4").alias("Start_Time_Day_4"),
    col("Schedule_Pattern_Weeks.wd:End_Time_Day_4").alias("End_Time_Day_4"),
    col("Schedule_Pattern_Weeks.wd:Start_Time_Day_5").alias("Start_Time_Day_5"),
    col("Schedule_Pattern_Weeks.wd:End_Time_Day_5").alias("End_Time_Day_5"),
    col("Schedule_Pattern_Weeks.wd:Start_Time_Day_6").alias("Start_Time_Day_6"),
    col("Schedule_Pattern_Weeks.wd:End_Time_Day_6").alias("End_Time_Day_6"),
    col("Schedule_Pattern_Weeks.wd:Start_Time_Day_7").alias("Start_Time_Day_7"),
    col("Schedule_Pattern_Weeks.wd:End_Time_Day_7").alias("End_Time_Day_7")
)

# Display the DataFrame
flattened_df.show()
