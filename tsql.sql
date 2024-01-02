SELECT
    p.person_id,
    p.basic_hourly_rate AS basic,
    COALESCE(SUM(CASE WHEN sub.pc_pay_code = 'premium_1' THEN CAST(sub.pc_value AS FLOAT) ELSE 0.0 END), 0.0) AS premium_1,
    COALESCE(SUM(CASE WHEN sub.pc_pay_code = 'premium_2' THEN CAST(sub.pc_value AS FLOAT) ELSE 0.0 END), 0.0) AS premium_2,
    COALESCE(SUM(CASE WHEN sub.pc_pay_code = 'premium_3' THEN CAST(sub.pc_value AS FLOAT) ELSE 0.0 END), 0.0) AS premium_3,
    COALESCE(SUM(CAST(sub.pc_value AS FLOAT)), 0.0) AS total_pay
FROM
    person p
LEFT JOIN
    (SELECT person_id, pay_code AS pc_pay_code, value AS pc_value FROM pay_codes) sub
ON
    p.person_id = sub.person_id
GROUP BY
    p.person_id, p.basic_hourly_rate;