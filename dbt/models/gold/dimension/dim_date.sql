# dbt/models/gold/dimensions/dim_date.sql
-- dbt/models/gold/dimensions/dim_date.sql
{{ config(materialized='table', tags=['gold','dimension','date']) }}
 
WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart='day',
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2025-12-31' as date)"
    )}}
)
 
SELECT
    CAST(DATE_FORMAT(date_day, 'yyyyMMdd') AS INT)  AS date_key,
    date_day                                        AS date_actual,
    YEAR(date_day)                                  AS year,
    QUARTER(date_day)                               AS quarter,
    MONTH(date_day)                                 AS month,
    DATE_FORMAT(date_day, 'MMMM')                   AS month_name,
    WEEKOFYEAR(date_day)                            AS week_of_year,
    DAYOFWEEK(date_day)                             AS day_of_week,
    DATE_FORMAT(date_day, 'EEEE')                   AS day_name,
    DAYOFMONTH(date_day)                            AS day_of_month,
    (DAYOFWEEK(date_day) IN (1, 7))                 AS is_weekend,
    (MONTH(date_day) = 1  AND DAYOFMONTH(date_day) = 1)   AS is_new_year,
    (MONTH(date_day) = 7  AND DAYOFMONTH(date_day) = 14)  AS is_bastille_day,
    (MONTH(date_day) = 12 AND DAYOFMONTH(date_day) = 25)  AS is_christmas,
    (MONTH(date_day) = 11 AND DAYOFWEEK(date_day) = 6
     AND DAYOFMONTH(date_day) BETWEEN 23 AND 29)   AS is_black_friday,
    (MONTH(date_day) IN (11, 12))                  AS is_holiday_season,
    DATE_FORMAT(date_day, 'yyyy-MM')               AS year_month,
    CONCAT('Q', QUARTER(date_day), ' ', YEAR(date_day)) AS year_quarter
FROM date_spine
