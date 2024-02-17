from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, current_date, date_sub, substring, when, concat_ws, year, month
from pyspark.sql.window import Window
from delta.tables import DeltaTable

class PayrollDataProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.alpha_account_name = "storage_account_name"
        self.alpha_account_key = "storage_account_key"
        self.alpha_container_name = "storage_container_name"
        # Corrected variable names below
        self.alpha_storage_config = {"fs.azure.account.key." + self.alpha_account_name + ".blob.core.windows.net": self.alpha_account_key}
        self.alpha_storage_url = f"wasbs://{self.alpha_container_name}@{self.alpha_account_name}.blob.core.windows.net"

        
    def read_delta_table(self, path):
        table_path = f"{self.alpha_storage_url}/{path}"
        return self.spark.read.options(**self.alpha_storage_config).format("delta").load(table_path)

    def prefix_columns(self, df, prefix):
        for col_name in df.columns:
            df = df.withColumnRenamed(col_name, f"{prefix}_{col_name}")
        return df

    def process_colleague_rates(self, df):
        processed_df = df.drop("md_process_id","md_source_ts","md_created_ts","md_source_path").filter(col("start_date") <= current_date())
        return self.prefix_columns(processed_df, "cr")

    def process_colleague_base_rate(self, df):
        processed_df = df.drop("md_process_id","md_source_ts","md_created_ts","md_source_path").filter(col("effective_date") <= current_date())               
        return self.prefix_columns(processed_df, "cbr")

    def process_div_cc_hierarchy(self, df):
        processed_df = df.drop("md_process_id","md_source_ts","md_created_ts","md_source_path")
        return self.prefix_columns(processed_df, "dcch")
    
    def process_colleague_worked_hours(self, df):
        processed_df = df.drop("md_process_id", "md_source_ts", "md_created_ts", "md_source_path")
        return self.prefix_columns(processed_df, "cwh")

    def process_wd_wb_mapping(self, processed_df):
        return self.prefix_columns(processed_df, "wdwbmap")
    
    def process_calendar(self, df):
        processed_df = df.drop("md_process_id","md_source_ts","md_created_ts","md_source_path")
        return prosessed_df

    def transform_colleague_worked_hours(self, df):
        cwh.createOrReplaceTempView("cwh")
        cd.createOrReplaceTempView("cd")
        cd1.createOrReplaceTempView("cd1")
        
        query = """
        SELECT
            cwh.cwh_dock_name,
            substring(cast(cwh.cwh_dock_name as string), 1, 4) as cwh_store_number, 
            substring(cast(cwh.cwh_dock_name as string), 5, 8) as cwh_division,
            cwh_emp_name,
            cwh.cwh_wrkd_hrs,
            cwh.cwh_wrks_workdate,
            cwh.cwh_wrkd_Start_time,
            cwh.cwh_wrkd_end_time,
            cwh.cwh_tcode_name,
            cwh.cwh_htype_name,
            cwh.cwh_emp_val4,
            from 
            cwh
            join cd
            on cwh.cwh_wrks_work_date = cd.calendar_date
            where exists (
                select * from cd1 
                where cd1.wm_week = cd.wm_week
                and cd1.calendar_year = cd.calendar_year
                and cd1.calendar_date = current_date() -1 day
                
            )
        """
        result_df = self.spark.sql(query)
        return result_df

    def create_temp_views(self):
        # Assuming DataFrames are already loaded as attributes of the spark session object
        self.spark.cwh_processed.createOrReplaceTempView("cwh_processed")
        self.spark.wd_wb_map_processed.createOrReplaceTempView("wd_wb_mapping_processed")
        self.spark.cbr_processed.createOrReplaceTempView("cbr_processed")
        self.spark.cr_processed.createOrReplaceTempView("cr_processed")
        self.spark.div_cc_processed.createOrReplaceTempView("div_cc_processed")
        self.spark.cr_processed_a1.createOrReplaceTempView("cr_processed_a1")
        self.spark.cbr_processed_b1.createOrReplaceTempView("cbr_processed_b1")

        # Create additional temporary views for the maximum effective dates
        self.spark.sql("""
        CREATE OR REPLACE TEMP VIEW max_cr_start_date AS
        SELECT cr_colleague_id, cr_pay_code, MAX(cr_start_date) as max_start_date
        FROM cr_processed_a1
        GROUP BY cr_colleague_id, cr_pay_code
        """)

        self.spark.sql("""
        CREATE OR REPLACE TEMP VIEW max_cbr_effective_date AS
        SELECT cbr_colleague_id, MAX(cbr_effective_date) as max_effective_date
        FROM cbr_processed_b1
        GROUP BY cbr_colleague_id
        """)

    def transform_colleague_rates(self):
        # Ensure all temp views are created
        self.create_temp_views()

        # Final query using temporary views
        query = """
        SELECT
            k.date_key,
            k.store_number,
            dcc.vaw_division,
            COALESCE(k.wrk_hrs, 0.00) as Actual_Hours,
            COALESCE(SUM(k.calculated_wages), 0.00) as Actual_Wages
        FROM
            (SELECT
                c.cwh_wrks_work_date as date_key,
                c.cwh_store_number as store_number,
                c.cwh_division,
                c.cwh_tcode_name,
                c.cwh_htype_name,
                c.cwh_emp_val4,
                d.wdwbmap_pay_code as mp_pay_code,
                c.cwh_emp_name as wb_cid,
                a.cr_colleague_id as wd_bcid,
                b.cbr_cost_centre,
                c.cwh_wrkd_hrs,
                a.cr_pay_code as wd_pay_code,
                a.cr_pay_unit,
                CASE
                    WHEN d.wdwbmap_pay_code <> 'R010' THEN CAST(b.cbr_basic_hourly_rate AS numeric(6,2)) + CAST(a.cr_value AS numeric(6,2)) END AS premium_rate,
                CASE
                    WHEN d.wdwbmap_pay_code = 'R010' THEN b.cbr_basic_hourly_rate END AS basic_rate,
                CASE
                    WHEN (d.wdwbmap_pay_code = 'R010' AND d.wdwbmap_double_flag IS NULL) THEN CAST(b.cbr_basic_hourly_rate AS numeric(6,2)) * CAST(c.cwh_wrkd_hrs AS numeric(6,2))
                    WHEN (d.wdwbmap_pay_code = 'R010' AND d.wdwbmap_double_flag = 'Y') THEN 2 * CAST(b.cbr_basic_hourly_rate AS numeric(6,2)) * CAST(c.cwh_wrkd_hrs AS numeric(6,2))
                    WHEN (d.wdwbmap_pay_code <> 'R010' AND d.wdwbmap_double_flag IS NULL) THEN (CAST(b.cbr_basic_hourly_rate AS numeric(6,2)) + CAST(a.cr_value AS numeric(6,2))) * CAST(c.cwh_wrkd_hrs AS numeric(6,2))
                    WHEN (d.wdwbmap_pay_code <> 'R010' AND d.wdwbmap_double_flag = 'Y') THEN 2 * (CAST(b.cbr_basic_hourly_rate AS numeric(6,2)) + CAST(a.cr_value AS numeric(6,2))) * CAST(c.cwh_wrkd_hrs AS numeric(6,2))
                END AS calculated_wages,
                d.wdwbmap_double_flag,
                b.cbr_effective_date,
                b.cbr_job_code,
                c.cwh_wrkd_Start_time,
                c.cwh_wrkd_end_time
            FROM
                cwh_processed c
                LEFT JOIN wd_wb_mapping_processed d ON c.cwh_tcode_name = d.wdwbmap_tcode_name
                AND c.cwh_htype_name = d.wdwbmap_htype_name
                AND c.cwh_emp_val4 = d.wdwbmap_emp_val4
                LEFT JOIN cr_processed a ON d.wdwbmap_pay_code = a.cr_pay_code
                AND c.cwh_emp_name = a.cr_colleague_id
                LEFT JOIN max_cr_start_date mcsd ON a.cr_colleague_id = mcsd.cr_colleague_id
                AND a.cr_pay_code = mcsd.cr_pay_code
                AND a.cr_start_date = mcsd.max_start_date
                LEFT JOIN cbr_processed b ON b.cbr_colleague_id = c.cwh_emp_name
                AND b.cbr_effective_date <= c.cwh_wrks_work_date
                LEFT JOIN max_cbr_effective_date mced ON b.cbr_colleague_id = mced.cbr_colleague_id
                AND b.cbr_effective_date = mced.max_effective_date) k
            LEFT JOIN div_cc_processed dcc ON k.cwh_division = dcc.dcch_cc_mapping
        GROUP BY
            k.date_key, k.store_number, dcc.vaw_division
        ORDER BY k.date_key, k.store_number, dcc.vaw_division
        """

        result_df = self.spark.sql(query)
        return result_df

    
    
    
    
    
    
    
    
    
    
    
    def create_temp_views(cwh_processed, wd_wb_map_processed, cbr_processed, cr_processed, div_cc_processed, cr_processed_a1, cbr_processed_b1):
        cwh_processed.createOrReplaceTempView("cwh_processed")
        wd_wb_map_processed.createOrReplaceTempView("wd_wb_mapping_processed")
        cbr_processed.createOrReplaceTempView("cbr_processed")
        cr_processed.createOrReplaceTempView("cr_processed")
        div_cc_processed.createOrReplaceTempView("div_cc_processed")
        cr_processed_a1.createOrReplaceTempView("cr_processed_a1")
        cbr_processed_b1.createOrReplaceTempView("cbr_processed_b1")

        def transform_colleague_rates(session):
            create_temp_views(session.cwh_processed, session.wd_wb_map_processed, session.cbr_processed,
                      session.cr_processed, session.div_cc_processed, session.cr_processed_a1,
                      session.cbr_processed_b1)

        query = """
        SELECT
            k.date_key,
            k.store_number,
            dcc.vaw_division,
            COALESCE(k.wrk_hrs, 0.00) as Actual_Hours,
            COALESCE(SUM(k.calculated_wages), 0.00) as Actual_Wages
        FROM
            (SELECT
                c.cwh_wrks_work_date as date_key,
                c.cwh_store_number as store_number,
                c.cwh_division,
                c.cwh_tcode_name,
                c.cwh_htype_name,
                c.cwh_emp_val4,
                d.wdwbmap_pay_code as mp_pay_code,
                c.cwh_emp_name as wb_cid,
                a.cr_colleague_id as wd_bcid,
                b.cbr_cost_centre,
                c.cwh_wrkd_hrs,
                a.cr_pay_code as wd_pay_code,
                a.cr_pay_unit,
                CASE
                    WHEN d.wdwbmap_pay_code <> 'R010' THEN CAST(b.cbr_basic_hourly_rate AS numeric(6,2)) + CAST(a.cr_value AS numeric(6,2)) END AS premium_rate,
                CASE
                    WHEN d.wdwbmap_pay_code = 'R010' THEN b.cbr_basic_hourly_rate END AS basic_rate,
                CASE
                    WHEN (d.wdwbmap_pay_code = 'R010' AND d.wdwbmap_double_flag IS NULL) THEN CAST(b.cbr_basic_hourly_rate AS numeric(6,2)) * CAST(c.cwh_wrkd_hrs AS numeric(6,2))
                    WHEN (d.wdwbmap_pay_code = 'R010' AND d.wdwbmap_double_flag = 'Y') THEN 2 * CAST(b.cbr_basic_hourly_rate AS numeric(6,2)) * CAST(c.cwh_wrkd_hrs AS numeric(6,2))
                    WHEN (d.wdwbmap_pay_code <> 'R010' AND d.wdwbmap_double_flag IS NULL) THEN (CAST(b.cbr_basic_hourly_rate AS numeric(6,2)) + CAST(a.cr_value AS numeric(6,2))) * CAST(c.cwh_wrkd_hrs AS numeric(6,2))
                    WHEN (d.wdwbmap_pay_code <> 'R010' AND d.wdwbmap_double_flag = 'Y') THEN 2 * (CAST(b.cbr_basic_hourly_rate AS numeric(6,2)) + CAST(a.cr_value AS numeric(6,2))) * CAST(c.cwh_wrkd_hrs AS numeric(6,2))
                END AS calculated_wages,
                d.wdwbmap_double_flag,
                b.cbr_effective_date,
                b.cbr_job_code,
                c.cwh_wrkd_Start_time,
                c.cwh_wrkd_end_time
            FROM
                cwh_processed AS c
                LEFT OUTER JOIN wd_wb_map_processed AS d ON c.cwh_tcode_name = d.wdwbmap_tcode_name
                AND c.cwh_htype_name = d.wdwbmap_htype_name
                AND c.cwh_emp_val4 = d.wdwbmap_emp_val4
                LEFT OUTER JOIN cr_processed AS a ON d.wdwbmap_pay_code = a.cr_pay_code
                AND a.cr_colleague_id = c.cwh_emp_name 
                AND a.cr_start_date <= c.cwh_wrks_work_date
                AND a.cr_start_date = (SELECT MAX(cr_start_date) FROM cr_processed_a1 
                                       WHERE a1.cr_colleague_id = a.cr_colleague_id
                                       AND a1.cr_pay_code = a.cr_pay_code
                                       AND a1.cr_start_date <= c.cwh_wrks_work_date)
                LEFT OUTER JOIN cbr_processed AS b ON 
                    b.cbr_colleague_id = c.cwh_emp_name
                AND b.cbr_effective_date <= c.cwh_wrks_work_date
                AND b.cbr_effective_date = (SELECT MAX(cbr_effective_date) FROM cbr_processed_b1
                                            WHERE b1.cbr_colleague_id = b.cbr_colleague_id
                                            AND b1.cbr_effective_date <= c.cwh_wrks_work_date)) k
            LEFT OUTER JOIN div_cc_processed AS dcc ON k.cwh_division = dcc.dcch_cc_mapping
        GROUP BY
            k.date_key, k.store_number, dcc.vaw_division
        ORDER BY k.date_key, k.store_number, dcc.vaw_division
        """
        
        result_df = self.spark.sql(query)
        return result_df
    
    
    from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, max as max_

def transform_colleague_rates(spark, cwh_processed, wd_wb_map_processed, cbr_processed, cr_processed, div_cc_processed, cr_processed_a1, cbr_processed_b1):
    # Join operations equivalent to the initial part of the SQL query
    k = cwh_processed.alias("c") \
        .join(wd_wb_map_processed.alias("d"), 
              (col("c.cwh_tcode_name") == col("d.wdwbmap_tcode_name")) & 
              (col("c.cwh_htype_name") == col("d.wdwbmap_htype_name")) & 
              (col("c.cwh_emp_val4") == col("d.wdwbmap_emp_val4")), 
              "left_outer") \
        .join(cr_processed.alias("a"), 
              (col("d.wdwbmap_pay_code") == col("a.cr_pay_code")) & 
              (col("c.cwh_emp_name") == col("a.cr_colleague_id")) & 
              (col("a.cr_start_date") <= col("c.cwh_wrks_work_date")), 
              "left_outer") \
        .join(cbr_processed.alias("b"), 
              (col("b.cbr_colleague_id") == col("c.cwh_emp_name")) & 
              (col("b.cbr_effective_date") <= col("c.cwh_wrks_work_date")), 
              "left_outer") \
        .withColumn("max_cr_start_date", 
                    max_("a.cr_start_date").over(Window.partitionBy("a.cr_colleague_id", "a.cr_pay_code").orderBy(col("a.cr_start_date").desc()))) \
        .withColumn("max_cbr_effective_date", 
                    max_("b.cbr_effective_date").over(Window.partitionBy("b.cbr_colleague_id").orderBy(col("b.cbr_effective_date").desc()))) \
        .filter((col("a.cr_start_date") == col("max_cr_start_date")) & (col("b.cbr_effective_date") == col("max_cbr_effective_date"))) \
        .selectExpr("c.cwh_wrks_work_date as date_key", 
                    "c.cwh_store_number as store_number", 
                    "c.cwh_division", 
                    "d.wdwbmap_pay_code as mp_pay_code", 
                    "c.cwh_emp_name as wb_cid", 
                    "a.cr_colleague_id as wd_bcid", 
                    "b.cbr_cost_centre", 
                    "c.cwh_wrkd_hrs", 
                    "a.cr_pay_code as wd_pay_code", 
                    "a.cr_pay_unit", 
                    "CASE WHEN d.wdwbmap_pay_code <> 'R010' THEN b.cbr_basic_hourly_rate + a.cr_value END AS premium_rate",
                    "CASE WHEN d.wdwbmap_pay_code = 'R010' THEN b.cbr_basic_hourly_rate END AS basic_rate",
                    """CASE
                        WHEN d.wdwbmap_pay_code = 'R010' AND d.wdwbmap_double_flag IS NULL THEN b.cbr_basic_hourly_rate * c.cwh_wrkd_hrs
                        WHEN d.wdwbmap_pay_code = 'R010' AND d.wdwbmap_double_flag = 'Y' THEN 2 * b.cbr_basic_hourly_rate * c.cwh_wrkd_hrs
                        WHEN d.wdwbmap_pay_code <> 'R010' AND d.wdwbmap_double_flag IS NULL THEN (b.cbr_basic_hourly_rate + a.cr_value) * c.cwh_wrkd_hrs
                        WHEN d.wdwbmap_pay_code <> 'R010' AND d.wdwbmap_double_flag = 'Y' THEN 2 * (b.cbr_basic_hourly_rate + a.cr_value) * c.cwh_wrkd_hrs
                      END AS calculated_wages""")

    # Join with div_cc_processed for the division mapping
    result_df = k.join(div_cc_processed.alias("dcc"), col("k.cwh_division") == col("dcc.dcch_cc_mapping"), "left_outer") \
                 .groupBy("date_key", "store_number", "dcc.vaw_division") \
                 .agg(F.sum("cwh_wrkd_hrs").alias("Actual_Hours"), 
                      F.sum("calculated_wages").alias("Actual_Wages")) \
                 .orderBy("date_key", "store_number", "dcc.vaw_division")

    return result_df

    
    if __name__ == "__main__":
    
        spark = SparkSession.builder.appName("PayrollDataProcessor").getOrCreate()
        processor = PayrollDataProcessor(spark)
    
        colleague_rates_path = "colleague_rates"
        cr =  processor.read_delta_table(colleague_rates_path)
        cr_processed = processor.process_colleague_rates(cr)
    
        colleague_rates_path = "colleague_rates"
        cr1 =  processor.read_delta_table(colleague_rates_path)
        cr_processed_a1 = processor.process_colleague_rates(cr1)

        colleague_base_rate_path = "colleague_base_rate"
        cbr =  processor.read_delta_table(colleague_base_rate_path)
        cbr_processed = processor.process_colleague_base_rate(cbr)
    
        colleague_base_rate_path = "colleague_base_rate"
        cbr1 =  processor.read_delta_table(colleague_base_rate_path)
        cbr_processed_b1 = processor.process_colleague_base_rate(cbr1)
    
        div_cc_hierarchy_path = "div_cc_hierarchy"
        div_cc =  processor.read_delta_table(div_cc_hierarchy_path)
        div_cc_processed = processor.process_div_cc_hierarchy(div_cc)
    
        colleague_worked_hours_path = "colleague_worked_hours"
        cwh =  processor.read_delta_table(colleague_worked_hours_path)
        cwh_processed = processor.process_colleague_worked_hours(cwh)
    
        wd_wb_mapping_path = "wd_wb_mapping"
        wd_wb_map =  processor.read_delta_table(wd_wb_mapping_path)
        wd_wb_map_processed = processor.process_wd_wb_mapping(wd_wb_map)
    
        dim_calendar_path = "dim_calendar"
        dim_cc =  processor.read_delta_table(dim_calendar_path)
        dim_cc_processed = processor.process_calendar(dim_cc)
    
        result_df = processor.transform_colleague_worked_hours(cwh_processed, wd_wb_map_processed, cbr_processed, cr_processed, div_cc_processed, cr_processed_a1, cbr_processed_b1)
        result_df.show()
    
    
    
    def transform_colleague_rates(session):
    # Step 1: Create necessary temporary views from session data
    create_temp_views(session.cwh_processed, session.wd_wb_map_processed, session.cbr_processed,
                      session.cr_processed, session.div_cc_processed, session.cr_processed_a1,
                      session.cbr_processed_b1)

    # Step 2: Create temporary view for the max cr_start_date
    session.spark.sql("""
    CREATE OR REPLACE TEMP VIEW max_cr_start_date AS
    SELECT cr_colleague_id, cr_pay_code, MAX(cr_start_date) as max_start_date
    FROM cr_processed_a1
    GROUP BY cr_colleague_id, cr_pay_code
    """)
    
    # Step 3: Create temporary view for the max cbr_effective_date
    session.spark.sql("""
    CREATE OR REPLACE TEMP VIEW max_cbr_effective_date AS
    SELECT cbr_colleague_id, MAX(cbr_effective_date) as max_effective_date
    FROM cbr_processed_b1
    GROUP BY cbr_colleague_id
    """)

    # Step 4: Execute the main query with joins to the above temporary views instead of correlated subqueries
    query = """
    SELECT
        k.date_key,
        k.store_number,
        dcc.vaw_division,
        COALESCE(k.wrk_hrs, 0.00) as Actual_Hours,
        COALESCE(SUM(k.calculated_wages), 0.00) as Actual_Wages
    FROM
        (SELECT
            c.cwh_wrks_work_date as date_key,
            c.cwh_store_number as store_number,
            c.cwh_division,
            c.cwh_tcode_name,
            c.cwh_htype_name,
            c.cwh_emp_val4,
            d.wdwbmap_pay_code as mp_pay_code,
            c.cwh_emp_name as wb_cid,
            a.cr_colleague_id as wd_bcid,
            b.cbr_cost_centre,
            c.cwh_wrkd_hrs as wrk_hrs,
            a.cr_pay_code as wd_pay_code,
            a.cr_pay_unit,
            CASE
                WHEN d.wdwbmap_pay_code <> 'R010' THEN CAST(b.cbr_basic_hourly_rate AS numeric(6,2)) + CAST(a.cr_value AS numeric(6,2))
                WHEN d.wdwbmap_pay_code = 'R010' THEN b.cbr_basic_hourly_rate
            END AS rate,
            CASE
                WHEN (d.wdwbmap_double_flag = 'Y') THEN 2 * CAST(c.cwh_wrkd_hrs AS numeric(6,2)) * rate
                ELSE CAST(c.cwh_wrkd_hrs AS numeric(6,2)) * rate
            END AS calculated_wages
        FROM
            cwh_processed AS c
            LEFT JOIN wd_wb_map_processed AS d ON c.cwh_tcode_name = d.wdwbmap_tcode_name AND c.cwh_htype_name = d.wdwbmap_htype_name AND c.cwh_emp_val4 = d.wdwbmap_emp_val4
            LEFT JOIN cr_processed AS a ON d.wdwbmap_pay_code = a.cr_pay_code AND c.cwh_emp_name = a.cr_colleague_id
            LEFT JOIN max_cr_start_date AS mcsd ON a.cr_colleague_id = mcsd.cr_colleague_id AND a.cr_pay_code = mcsd.cr_pay_code AND a.cr_start_date = mcsd.max_start_date
            LEFT JOIN cbr_processed AS b ON b.cbr_colleague_id = c.cwh_emp_name
            LEFT JOIN max_cbr_effective_date AS mced ON b.cbr_colleague_id = mced.cbr_colleague_id AND b.cbr_effective_date = mced.max_effective_date) k
    LEFT JOIN div_cc_processed AS dcc ON k.cwh_division = dcc.dcch_cc_mapping
    GROUP BY
        k.date_key, k.store_number, dcc.vaw_division
    ORDER BY k.date_key, k.store_number, dcc.vaw_division
    """

    result_df = session.spark.sql(query)
    return result_df

    
            
       