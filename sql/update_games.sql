UPDATE ud_games g
INNER JOIN
(
    SELECT    g.game_id   game_id,
              s.game_ts   game_ts,
              s.scrape_ts scrape_ts
    FROM      ncaaf_es_scrape s
    LEFT JOIN ud_games g
              ON s.game_id = g.espn_game_id
    WHERE     scrape_ts = (SELECT max(scrape_ts) max_scrape_ts
                           FROM ncaaf_es_scrape WHERE week_id = 2)
    ORDER BY 2
) es
ON g.game_id = es.game_id
SET g.game_ts = es.game_ts,
    g.update_ts = es.scrape_ts
;
