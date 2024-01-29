-- colleague_worked_hours as c
-- wd_wb_mapping as d
-- colleague_rates as a
-- colleague_base_rate as b

with colleague_rates AS
(
    select colleague_id,
    company_number,
    pay_code,
    [start_date],
    end_date,
    pay_unit,
    value,
    row_number() over partition by colleague_id,pay_code,[start_date] order by [start_date] desc rate_seq
    from wd.wd.wd_colleage_rates
    where convert(date,[start_date],103)<=convert(date,getdate(),103)
),

colleaguye_base_rate as
(
    select 
    record_type,
    colleague_id,
    effective_date,
    cost_center,
    job_code,
    basic_hourly_rate,
    row_number() over partition by colleague_id,effective_date order by effective_date desc rate_seq
    from wd.wd.wd_colleague_base_rate
    where convert(date,effective_date,103)<=convert(date,getdate(),103)
),

colleague_worked_hours 
AS
(
    Select 
    dock_name, 
    cast(substring(dock_name,1,4)as Int) store_number, 
    substring(dock_name,5,8) division, 
    cast(substring(dock_name,5,8) as INT ) division_int,
    emp_name as colleague_id, 
    wrkd_hrs,
    convert(date,work_start_time,103) datekey,
    wrkd_start_time,
    wrkd_end_time,
    htype_name,
    tcode_name, 
    emp_val4,
    htype_multiple
    from wb.wb.wb_colleague_hours
    where convert(date,work_start_time,103)=convert(date,getdate()-4,103)

),

wd_wb_mapping
AS
(
    select 
    distinct pay_code,
    htype_name,
    tcode_name, 
    emp_val4,
    double_flag
    from rf.vaw.vaw_wd_wb_mapping
)

select * from 
(
    select 
    c.datekey,
    c.store_number
    c.division,
    c.tcode_name,
    c.htype_name,
    c.htype_multiple,
    c.emp_val4
    c.colleague_id as wb_cid,
    d.pay_code as mp_pay_code,
    a.colleague_id as wd_cid,
    b.colleague_id as wd_bcid,
    b.cost_center,
    c.wrkd_hrs,
    a.pay_code as wd_pay_code,
    a.pay_unit,
    case when d.pay_code <> 'R010' THEN (cast(B.basic_hourly_rate AS NUMERIC(6,2) + cast(a.value AS NUMERIC(6,2))) END premium_rate,
    case when d.pay_code = 'R010' THEN b.basic_hourly_rate else null END as basic_rate,
    case 
    when d.pay_code = 'R010' and d.double_flag is null then cast(b.basic_hourly_rate as NUMERIC(6,2)) * cast((c.wrkd_hrs as NUMERIC(6,2)) * cast(c.htype_multiple as NUMERIC(6,2)))
    when d.pay_code = 'R010' and d.double_flag = 'y' then 2 * cast(b.basic_hourly_rate as numeric(6,2)) +cast ((c.wrkd_hrs as numeric(6,2)))
    when d.pay_code <> 'R010' and d.double_flag is null then 2 * cast(a.value as numeric(6,2)) * cast (c.wrkd_hrs as numeric(6,2))
    when d.pay_code <> 'R010' and d.double_flag = 'y' then 2 * cast(a.value as numeric(6,2)) * cast (c.wrkd_hrs as numeric(6,2))
    else null end as calculated_wages,
    d.double_flag
    b.effective_date,
    b.job_code,
    c.wrkd_start_time,c.wrkd_end_time
    from colleague_worked_hours as calculated_wages
    left outer join wd_wb_mapping as d on d.tcode_name = c.tcode_name and d.htype_name = c.htype_name and d.emp_val4 = c.emp_val4
    left outer join colleague_rates as a on d.pay_code = a.pay_code and c.colleague_id = a.colleague_id and a.rate_seq = 1
    left outer join colleague_base_rate as b on trim(b.colleague_id) = trim(c.colleague_id) and b.rate_seq = 1 ) K
    order by datekey,store_number,division,emp_val4
    