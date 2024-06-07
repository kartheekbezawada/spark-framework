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
    col("wd:Schedule_Pattern_Weeks.wd:Week_Number.@wd:Descriptor").alias("Week_Descriptor"),
    col("wd:Schedule_Pattern_Weeks.wd:Start_Time_Day_1").alias("Start_Time_Day_1"),
    col("wd:Schedule_Pattern_Weeks.wd:End_Time_Day_1").alias("End_Time_Day_1"),
    col("wd:Schedule_Pattern_Weeks.wd:Start_Time_Day_2").alias("Start_Time_Day_2"),
    col("wd:Schedule_Pattern_Weeks.wd:End_Time_Day_2").alias("End_Time_Day_2"),
    col("wd:Schedule_Pattern_Weeks.wd:Start_Time_Day_3").alias("Start_Time_Day_3"),
    col("wd:Schedule_Pattern_Weeks.wd:End_Time_Day_3").alias("End_Time_Day_3"),
    col("wd:Schedule_Pattern_Weeks.wd:Start_Time_Day_4").alias("Start_Time_Day_4"),
    col("wd:Schedule_Pattern_Weeks.wd:End_Time_Day_4").alias("End_Time_Day_4"),
    col("wd:Schedule_Pattern_Weeks.wd:Start_Time_Day_5").alias("Start_Time_Day_5"),
    col("wd:Schedule_Pattern_Weeks.wd:End_Time_Day_5").alias("End_Time_Day_5"),
    col("wd:Schedule_Pattern_Weeks.wd:Start_Time_Day_6").alias("Start_Time_Day_6"),
    col("wd:Schedule_Pattern_Weeks.wd:End_Time_Day_6").alias("End_Time_Day_6"),
    col("wd:Schedule_Pattern_Weeks.wd:Start_Time_Day_7").alias("Start_Time_Day_7"),
    col("wd:Schedule_Pattern_Weeks.wd:End_Time_Day_7").alias("End_Time_Day_7")
)

# Display the DataFrame
flattened_df.show()
