- weekly_final.sql

DECLARE run_date DATE DEFAULT @run_date;

WITH t AS (
  SELECT
    CONCAT(CAST(EXTRACT(YEAR FROM CURRENT_DATE()) AS STRING), '-', CAST(EXTRACT(ISOWEEK FROM CURRENT_DATE()) AS STRING)) as year_week,
    SUM(number_of_strikes),
    run_date as last_week_from,
    CURRENT_DATETIME() as query_execution_time
  FROM
    `bigquery-public-data.noaa_lightning.lightning_*`
  WHERE
    day BETWEEN DATE_SUB(run_date, INTERVAL 1 week)
    AND run_date
)
SELECT * FROM t WHERE EXTRACT(DAYOFWEEK FROM run_date) = 1