-- To remove the duplicate data and filter those data those having status = FInish
SELECT
  * EXCEPT(row_num)
FROM (
  SELECT
    *,
    ROW_NUMBER () OVER (PARTITION BY id ORDER BY 'date' DESC) AS row_num
  FROM
    `football-leagues-analytics.football_dataset.fixtures`) AS x
WHERE
  x.row_num=1 and x.status='FT';