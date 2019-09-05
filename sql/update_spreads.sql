UPDATE     ud_spreads s
INNER JOIN 
(
    SELECT     g.game_id,
               CASE WHEN es.team_id IS NULL THEN os.team_id
                    ELSE es.team_id END team_id,
               CASE WHEN es.team_id IS NULL THEN os.spread
                    ELSE es.spread END spread,
               CASE WHEN es.team_id IS NULL THEN os.scrape_ts
                    ELSE es.scrape_ts END scrape_ts
    FROM      ud_games g
    LEFT JOIN (SELECT    g.game_id game_id,
                         t.atthletics_team_id team_id,
                         s.spread spread,
                         s.scrape_ts scrape_ts
               FROM      ncaaf_es_scrape s
               LEFT JOIN ud_games g
                         ON s.game_id = g.espn_game_id
               LEFT JOIN teams t
                         ON s.underdog_id = t.espn_team_id
               WHERE     scrape_ts = (SELECT max(scrape_ts) max_scrape_ts FROM ncaaf_es_scrape)
               ) es
               ON g.game_id = es.game_id
    LEFT JOIN (SELECT    g.game_id game_id,
                         t3.atthletics_team_id team_id,
                         s.spread spread,
                         s.scrape_ts scrape_ts
               FROM      ncaaf_os_scrape s
               LEFT JOIN teams t1
                         ON s.away_team = t1.os_team_name
               LEFT JOIN teams t2
                         ON s.home_team = t2.os_team_name
               LEFT JOIN teams t3
                         ON s.underdog = t3.os_team_name
               INNER JOIN ud_games g
                         ON t1.atthletics_team_id = g.away_id
                         AND t2.atthletics_team_id = g.home_id
               WHERE scrape_ts = (SELECT max(scrape_ts) max_scrape_ts FROM ncaaf_os_scrape)
               ) os
               ON g.game_id = os.game_id
               WHERE g.week_id = 2
) es_os
ON s.game_id = es_os.game_id
SET s.spread = es_os.spread,
    s.process_ts = es_os.scrape_ts
;
