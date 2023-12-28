SELECT
  * EXCEPT(row_num)
FROM (
  SELECT
    *,
    ROW_NUMBER () OVER (PARTITION BY fixture_id,team_id,player_id) AS row_num
  FROM
    `football-leagues-analytics.football_dataset.match-lineups`) AS x
WHERE
  x.row_num=1;