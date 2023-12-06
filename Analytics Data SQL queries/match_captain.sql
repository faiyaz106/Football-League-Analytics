SELECT
  * EXCEPT(row_num)
FROM (
  SELECT
    *,
    ROW_NUMBER () OVER (PARTITION BY fixture_id,team_id) AS row_num
  FROM
    `football-leagues-analytics.football_dataset.match-captain`) AS x
WHERE
  x.row_num=1;