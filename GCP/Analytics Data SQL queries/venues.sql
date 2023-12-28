SELECT
  * EXCEPT(row_num)
FROM (
  SELECT
    *,
    ROW_NUMBER () OVER (PARTITION BY id ORDER BY CAPACITY DESC) AS row_num
  FROM
    `football-leagues-analytics.football_dataset.venues`) AS x
WHERE
  x.row_num=1;
