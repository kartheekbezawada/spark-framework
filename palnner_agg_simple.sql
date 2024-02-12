SELECT 
    A.Store_nbr,
    A.Division,
    A.Today_Date,
    A.Week_Day,
    A.year,
    A.Week_Nbr,
    A.FS_Weekly_Sales,
    A.FS_Weekly_Hrs,
    A.FS_Wkly_Cost,
    A.FS_Wkly_Cost_Hour,
    CASE 
        WHEN A.Week_Day = 'Saturday' THEN A.FS_Fri_Sales
        WHEN A.Week_Day = 'Sunday' THEN A.FS_Sat_Sales
        WHEN A.Week_Day = 'Monday' THEN A.FS_Sun_Sales
        WHEN A.Week_Day = 'Tuesday' THEN A.FS_Mon_Sales
        WHEN A.Week_Day = 'Wednesday' THEN A.FS_Tue_Sales
        WHEN A.Week_Day = 'Thursday' THEN A.FS_Wed_Sales
        WHEN A.Week_Day = 'Friday' THEN A.FS_Thu_Sales
    END AS Previous_Day_Sales,
    CASE 
        WHEN A.Week_Day = 'Saturday' THEN A.FH_Fri_Hrs
        WHEN A.Week_Day = 'Sunday' THEN A.FH_Sat_Hrs
        WHEN A.Week_Day = 'Monday' THEN A.FH_Sun_Hrs
        WHEN A.Week_Day = 'Tuesday' THEN A.FH_Mon_Hrs
        WHEN A.Week_Day = 'Wednesday' THEN A.FH_Tue_Hrs
        WHEN A.Week_Day = 'Thursday' THEN A.FH_Wed_Hrs
        WHEN A.Week_Day = 'Friday' THEN A.FH_Thu_Hrs
    END AS Previous_Day_Hrs,
    CASE 
        WHEN A.Week_Day = 'Saturday' THEN A.FC_Fri_Cost
        WHEN A.Week_Day = 'Sunday' THEN A.FC_Sat_Cost
        WHEN A.Week_Day = 'Monday' THEN A.FC_Sun_Cost
        WHEN A.Week_Day = 'Tuesday' THEN A.FC_Mon_Cost
        WHEN A.Week_Day = 'Wednesday' THEN A.FC_Tue_Cost
        WHEN A.Week_Day = 'Thursday' THEN A.FC_Wed_Cost
        WHEN A.Week_Day = 'Friday' THEN A.FC_Thu_Cost
    END AS Previous_Day_Cost,
    CASE 
        WHEN A.Week_Day = 'Saturday' THEN A.Fri_Cph
        WHEN A.Week_Day = 'Sunday' THEN A.Sat_Cph
        WHEN A.Week_Day = 'Monday' THEN A.Sun_Cph
        WHEN A.Week_Day = 'Tuesday' THEN A.Mon_Cph
        WHEN A.Week_Day = 'Wednesday' THEN A.Tue_Cph
        WHEN A.Week_Day = 'Thursday' THEN A.Wed_Cph
        WHEN A.Week_Day = 'Friday' THEN A.Thu_Cph
    END AS Previous_Day_CPH


From 

(SELECT 
    store_nbr AS Store_nbr,
    division AS Division,
    CONVERT(DATE, GETDATE()) AS Today_Date,
    DATENAME(WEEKDAY, GETDATE()) AS Week_Day,
    yr AS year,
    wk_nbr AS Week_Nbr,
    wkly_sales AS FS_Weekly_Sales,
    wkly_hrs AS FS_Weekly_Hrs,
    wkly_cost AS FS_Wkly_Cost,
    wkly_var_hrs AS FS_Weekly_Var_Hrs,
    wkly_cost AS FS_Wkly_Cost_Hour, -- Note: This might need correction to reflect actual cost per hour calculation
    -- Daily Sales Columns
    sat_sales AS FS_Sat_Sales_Percent,
    CAST(wkly_sales AS FLOAT) / 100 * sat_sales AS FS_Sat_Sales,
    sun_sales AS FS_Sun_Sales_Percent,
    CAST(wkly_sales AS FLOAT) / 100 * sun_sales AS FS_Sun_Sales,
    mon_sales AS FS_Mon_Sales_Percent,
    CAST(wkly_sales AS FLOAT) / 100 * mon_sales AS FS_Mon_Sales,
    tue_sales AS FS_Tue_Sales_Percent,
    CAST(wkly_sales AS FLOAT) / 100 * tue_sales AS FS_Tue_Sales,
    wed_sales AS FS_Wed_Sales_Percent,
    CAST(wkly_sales AS FLOAT) / 100 * wed_sales AS FS_Wed_Sales,
    thu_sales AS FS_Thu_Sales_Percent,
    CAST(wkly_sales AS FLOAT) / 100 * thu_sales AS FS_Thu_Sales,
    fri_sales AS FS_Fri_Sales_Percent,
    CAST(wkly_sales AS FLOAT) / 100 * fri_sales AS FS_Fri_Sales,
    -- Daily Hours Columns
    sat_hours AS FH_Sat_Hrs_Percent,
    CAST(wkly_hrs AS FLOAT) / 100 * sat_hours AS FH_Sat_Hrs,
    sun_hours AS FH_Sun_Hrs_Percent,
    CAST(wkly_hrs AS FLOAT) / 100 * sun_hours AS FH_Sun_Hrs,
    mon_hours AS FH_Mon_Hrs_Percent,
    CAST(wkly_hrs AS FLOAT) / 100 * mon_hours AS FH_Mon_Hrs,
    tue_hours AS FH_Tue_Hrs_Percent,
    CAST(wkly_hrs AS FLOAT) / 100 * tue_hours AS FH_Tue_Hrs,
    wed_hours AS FH_Wed_Hrs_Percent,
    CAST(wkly_hrs AS FLOAT) / 100 * wed_hours AS FH_Wed_Hrs,
    thu_hours AS FH_Thu_Hrs_Percent,
    CAST(wkly_hrs AS FLOAT) / 100 * thu_hours AS FH_Thu_Hrs,
    fri_hours AS FH_Fri_Hrs_Percent,
    CAST(wkly_hrs AS FLOAT) / 100 * fri_hours AS FH_Fri_Hrs,
    -- Daily Cost Columns
    sat_cost AS FC_Sat_Cost_Percent,
    CAST(wkly_cost AS FLOAT) / 100 * sat_cost AS FC_Sat_Cost,
    sun_cost AS FC_Sun_Cost_Percent,
    CAST(wkly_cost AS FLOAT) / 100 * sun_cost AS FC_Sun_Cost,
    mon_cost AS FC_Mon_Cost_Percent,
    CAST(wkly_cost AS FLOAT) / 100 * mon_cost AS FC_Mon_Cost,
    tue_cost AS FC_Tue_Cost_Percent,
    CAST(wkly_cost AS FLOAT) / 100 * tue_cost AS FC_Tue_Cost,
    wed_cost AS FC_Wed_Cost_Percent,
    CAST(wkly_cost AS FLOAT) / 100 * wed_cost AS FC_Wed_Cost,
    thu_cost AS FC_Thu_Cost_Percent,
    CAST(wkly_cost AS FLOAT) / 100 * thu_cost AS FC_Thu_Cost,
    fri_cost AS FC_Fri_Cost_Percent,
    CAST(wkly_cost AS FLOAT) / 100 * fri_cost AS FC_Fri_Cost,
    /*****************Daily Cost per Hours Columns ************/
    sat_cph as Sat_Cph,
    sun_cph as Sun_Cph,
    fri_cph as Fri_Cph,
    thu_cph as Thu_Cph,
    wed_cph as Wed_Cph,
    tue_cph as Tue_Cph,
    mon_cph as Mon_Cph
    
FROM 
    dbo.table as A)

    
    def process_wkly_planner_optimized(self, df):
        # Define the days of the week to be used in column names
        days_of_week = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    
        # Start with the initial transformations that don't fit the pattern
        df_transformed = df \
            .withColumn("Today_Date", F.current_date()) \
            .withColumn("Week_Day", F.date_format(F.current_date(), 'EEEE'))
        # Loop through the days of the week to apply transformations programmatically
        for day in days_of_week:
            sales_col = f"FS_{day}_Sales"
            hrs_col = f"FH_{day}_Hrs"
            cost_col = f"FC_{day}_Cost"
        # Sales
            df_transformed = df_transformed.withColumn(
                sales_col, df["wkly_sales"] / 100 * df[f"{day.lower()}_sales"])
            # Hours
            df_transformed = df_transformed.withColumn(
                hrs_col, df["wkly_hrs"] / 100 * df[f"{day.lower()}_hours"])
            # Cost
            df_transformed = df_transformed.withColumn(
                cost_col, df["wkly_cost"] / 100 * df[f"{day.lower()}_cost"])
    return df_transformed


    select ss_VAW_wtd_date,ss_store_nbr, ref_dept_nbr, ref_description, ref_division, ss_cd_wm_week, ss_total_sales, ref2_division,ref_description 
    from ss
    left join ref on ss.dept_nbr = ref.ref_dept_nbr
    left join ref2 on ss.division = ref2.ref2_division 
