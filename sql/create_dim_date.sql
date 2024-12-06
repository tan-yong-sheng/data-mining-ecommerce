CREATE OR REPLACE TABLE raw.d_date
PARTITION BY DATE_TRUNC(Date, MONTH) AS 
WITH date_holidays AS (
  SELECT 
    Date,
    DATE_TRUNC(Date, MONTH) AS first_day_of_the_month,
    EXTRACT(YEAR FROM Date) AS year,
    EXTRACT(WEEK FROM Date) AS week,
    EXTRACT(DAY FROM Date) AS day,
    FORMAT_DATE('%Q', Date) AS quarter,
    EXTRACT(MONTH FROM Date) AS month,
    EXTRACT(DAYOFWEEK FROM Date) AS day_of_week,  -- Added day_of_week
    IF(FORMAT_DATE('%A', Date) IN ('Saturday', 'Sunday'), FALSE, TRUE) AS is_weekday,
    -- Join with holiday table
    CASE 
       WHEN dh.date IS NOT NULL THEN TRUE
       ELSE FALSE
    END AS is_holiday
  FROM UNNEST(
    -- Set the needed time range here
    GENERATE_DATE_ARRAY('2016-09-01', '2025-12-31', INTERVAL 1 DAY)
  ) AS Date
  LEFT JOIN raw.d_holiday dh ON Date = dh.date
)
SELECT * FROM date_holidays;
