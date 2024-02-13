
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
from pyspark.sql.functions import when, col

# Initialize SparkSession (assuming you already have it as 'spark')
spark = SparkSession.builder.appName("UpdateDataFrameValue").getOrCreate()

# Sample DataFrame creation (assuming you already have your DataFrame)
# df = spark.createDataFrame([("12/02/2024", "val_b", "val_c", "val_d")], ["A", "B", "C", "D"])

# Update the value conditionally
df = df.withColumn("A", when(col("A") == "12/02/2024", "11/02/2024").otherwise(col("A")))

# Show the updated DataFrame
df.show()