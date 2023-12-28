SELECT
  * EXCEPT(row_num)
FROM (
  SELECT
    *,
    ROW_NUMBER () OVER (PARTITION BY id) AS row_num
  FROM
    `football-leagues-analytics.football_dataset.players`) AS x
WHERE
  x.row_num=1;