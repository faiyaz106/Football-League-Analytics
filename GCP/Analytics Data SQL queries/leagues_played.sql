SELECT
  * EXCEPT(row_num)
FROM (
  SELECT
    *,
    ROW_NUMBER () OVER (PARTITION BY league_id,start) AS row_num
  FROM
    `football-leagues-analytics.football_dataset.leagues-played`) AS x
WHERE
  x.row_num=1;