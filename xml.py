from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from datetime import date
from pyspark.dbutils import DBUtils

class Preprocessing:
    def __init__(self, spark):
        self.spark = spark
        
        # Create alpha Storage Account Connection String, Access key is stored in Azure Key Vault
        self.alpha_storage_account_name = "alphastorageaccount"
        self.alpha_container_name = "alphacontainer"
        self.alpha_key = "alphakey"
        self.alpha_access_key = dbutils.secrets.get(scope="keyvaultscope", key=self.alpha_key)
        self.alpha_storage_url = f"abfss://{self.alpha_container_name}@{self.alpha_storage_account_name}.dfs.core.windows.net"
        self.alpha_storage_url_http = f"https://{self.alpha_storage_account_name}.blob.core.windows.net"
        self.alpha_connection_string = f"DefaultEndpointsProtocol=https;AccountName={self.alpha_storage_account_name};AccountKey={self.alpha_access_key};EndpointSuffix=core.windows.net"
        
        # Create charlie Storage Account Connection String, Access key is stored in Azure Key Vault
        self.charlie_storage_account_name = "charliestorageaccount"
        self.charlie_container_name = "charliecontainer"
        self.charlie_key = "charliekey"
        self.charlie_access_key = dbutils.secrets.get(scope="keyvaultscope", key=self.charlie_key)
        self.charlie_storage_url = f"abfss://{self.charlie_container_name}@{self.charlie_storage_account_name}.dfs.core.windows.net"
        self.charlie_storage_url_http = f"https://{self.charlie_storage_account_name}.blob.core.windows.net"
        self.charlie_connection_string = f"DefaultEndpointsProtocol=https;AccountName={self.charlie_storage_account_name};AccountKey={self.charlie_access_key};EndpointSuffix=core.windows.net"
        
        self.set_config()
        
    def set_config(self):
        self.spark.conf.set("fs.azure.account.key." + self.alpha_storage_account_name + ".dfs.core.windows.net", self.alpha_access_key)
        self.spark.conf.set("fs.azure.account.key." + self.charlie_storage_account_name + ".dfs.core.windows.net", self.charlie_access_key)    

    def path_exists(self, file_path):
        full_path = f"{self.alpha_storage_url}/{file_path}"
        try:
            dbutils.fs.ls(full_path)
            return True
        except Exception as e:
            print(f"Full path {full_path} doesn't exist: {e}")
            return False

    def read_xml_files(self, file_path):
        if not self.path_exists(file_path):
            return None
        
        full_path = f"{self.alpha_storage_url}/{file_path}"
        files = dbutils.fs.ls(full_path)
        combined_df = None
        
        if len(files) == 0:
            print(f"No files available in the directory for {full_path}")
            return None
        
        for file in files:
            file_name = file.name
            df = self.spark.read.format("xml").option("rowTag", "wd:Report_Entry").load(f"{full_path}/{file_name}")
            
            if combined_df is None:
                combined_df = df
            else:
                combined_df = combined_df.union(df)
            
        return combined_df
        
if __name__ == "__main__":
    spark = SparkSession.builder.appName("UnzipAndCopyFiles").getOrCreate() 
    processor = Preprocessing(spark)
    
    current_date = date.today()
    current_year = current_date.strftime("%Y")
    current_month = current_date.strftime("%m")
    current_day = current_date.strftime("%d")
        
    file_path = f"folder1/subfolder1/subfolder2/{current_year}/{current_month}/{current_day}"
    
    df = processor.read_xml_files(file_path)
    
    if df is not None:
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
