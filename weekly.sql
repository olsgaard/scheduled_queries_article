SELECT
  SUM(number_of_strikes),
  @run_date as last_week_from
FROM
  `bigquery-public-data.noaa_lightning.lightning_*`
WHERE
  day BETWEEN DATE_SUB(@run_date, INTERVAL 1 week)
  AND @run_date
