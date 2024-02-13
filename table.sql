
create table dbo.a
( 
   emplid nvarchar(max) null)
  ,effdt datetime null
  ,work_hours nvarchar(max) null
  ,shift nvarchar(max) null
  ,std hrs decimal(6,2) null
  ,rownum bigint null
  ) on primary textimage_on primary
  go 


create table dbo.b
( 
   emplid bigint null)
  ,effdt datetime null
  ,work_hours nvarchar(20) null
  ,shift nvarchar(20) null
  ,std hrs decimal(6,2) null
  ,rownum bigint null
  ) on primary textimage_on primary
  
  nonclustered index ix_b on dbo.b (emplid)
  nonclustered index ix_b on dbo.b (effdt
  nonclustered index ix_b on dbo.b (rownum)
  nonclustered index ix_b on dbo.b (emplid, effdt, rownum)
  columnstore index ix_b on dbo.b (emplid)

  go


  -- Disable Indexes on dbo.b
ALTER INDEX ALL ON dbo.b DISABLE;

-- Declare variables for batch processing
DECLARE @BatchSize INT = 100000; -- Adjust the batch size based on your environment
DECLARE @RowsInserted INT = @BatchSize;

BEGIN TRY
    -- Loop to insert data in batches
    WHILE @RowsInserted = @BatchSize
    BEGIN
        BEGIN TRANSACTION;

        -- Insert data with conversion of emplid from NVARCHAR(MAX) to BIGINT
        INSERT INTO dbo.b (emplid, effdt, work_hours, shift, [std hrs], rownum)
            SELECT TOP (@BatchSize) 
                TRY_CONVERT(BIGINT, a.emplid) AS emplid,
                a.effdt, 
                a.work_hours, 
                a.shift, 
                a.[std hrs], 
                a.rownum
            FROM dbo.a AS a
            LEFT JOIN dbo.b AS b ON a.rownum = b.rownum -- Assuming 'rownum' can be used to avoid duplicates
            WHERE b.rownum IS NULL
            OPTION (TABLOCK);

        SET @RowsInserted = @@ROWCOUNT;

        COMMIT TRANSACTION;
    END

    -- Rebuild Indexes on dbo.b (Consider selective and conditional rebuilding based on your requirements)
    ALTER INDEX ALL ON dbo.b REBUILD;

END TRY
BEGIN CATCH
    -- Error handling: Print error message and rollback transaction if required
    PRINT 'Error occurred: ' + ERROR_MESSAGE();
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
END CATCH;

GO



SELECT
    a.*,
    b.*
FROM 
    TableA a
LEFT JOIN (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY JoinColumn ORDER BY SomeOrderColumn) AS rn
    FROM TableB
) b ON a.JoinColumn = b.JoinColumn AND b.rn = 1
WHERE
    a.SomeCondition = 'SomeValue'

from pyspark.sql import SparkSession
from pyspark.sql.functions import when

# Initialize Spark session
spark = SparkSession.builder.appName("DeltaTableUpdate").getOrCreate()

# Assuming your Delta table is located at 'path_to_delta_table'
delta_table_path = "/mnt/data/delta_table"

# Read the Delta table
df = spark.read.format("delta").load(delta_table_path)

# Perform the update using DataFrame transformations
df_updated = df.withColumn("A", when(col("A") == "12/02/2024", "11/02/2024").otherwise(col("A")))

# Show the updated DataFrame
df_updated.show()

# Write the updated DataFrame back to Delta format
df_updated.write.format("delta").mode("overwrite").save(delta_table_path)


select datekey,store_nbr,division,
round((wp_hrs + ((actual_sales - wp_sales)/1000) * wkly_var_hrs / 100),2) as forcecasted_hrs,
round(((wp_hrs + ((actual_sales - wp_sales)/1000) * wkly_var_hrs / 100) * wp_cost),2) as forecasted_wages
from a table 

class PayrollDataProcessor:
    # ... other parts of the class ...

    def joined_table(self):
        # Logic to create and return a joined DataFrame
        # For example:
        # return spark.sql("SELECT * FROM some_temp_view")
        pass

    def calculate_forecasted_metrics(self):
        # Get the DataFrame from the joined_table method
        df = self.joined_table()
        
        # Perform the calculations on the DataFrame
        df_forecasted = df.withColumn(
            "forecasted_hrs",
            round(col("wp_hrs") + (col("actual_sales") - col("wp_sales")) / 1000 * col("wkly_var_hrs") / 100, 2)
        ).withColumn(
            "forecasted_wages",
            round((col("wp_hrs") + ((col("actual_sales") - col("wp_sales")) / 1000) * col("wkly_var_hrs") / 100) * col("wp_cost"), 2)
        )
        
        # Return the transformed DataFrame
        return df_forecasted

# Usage:
# Assuming `spark` is the SparkSession
# processor = PayrollDataProcessor(spark)
# forecasted_df = processor.calculate_forecasted_metrics()
# forecasted_df.show()
