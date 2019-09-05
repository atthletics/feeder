INSERT INTO ud_spreads (game_id, team_id, spread)
SELECT
g.game_id,
CASE
   WHEN espn.team_id IS NULL THEN os.team_id
   ELSE espn.team_id
END team_id,
CASE
   WHEN espn.team_id IS NULL THEN os.spread
   ELSE espn.spread
END spread
FROM ud_games g
LEFT JOIN (
   select
   g.game_id game_id,
   t.atthletics_team_id team_id,
   s.spread spread
   from ncaaf_es_scrape s
   left join ud_games g
   on s.game_id = g.espn_game_id
   left join teams t
   on s.underdog_id = t.espn_team_id
   where scrape_ts = (SELECT max(scrape_ts) max_scrape_ts FROM ncaaf_es_scrape)
) espn
on g.game_id = espn.game_id
LEFT JOIN (
   SELECT
   g.game_id game_id,
   t3.atthletics_team_id team_id,
   s.spread spread
   from ncaaf_os_scrape s
   left join teams t1
   on s.away_team = t1.os_team_name
   left join teams t2
   on s.home_team = t2.os_team_name
   left join teams t3
   on s.underdog = t3.os_team_name
   inner join ud_games g
   on t1.atthletics_team_id = g.away_id
   and t2.atthletics_team_id = g.home_id
   where scrape_ts = (SELECT max(scrape_ts) max_scrape_ts FROM ncaaf_os_scrape)
) os
on g.game_id = os.game_id
WHERE g.week_id = 2
;
