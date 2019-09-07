UPDATE      ud_games g
INNER JOIN
(
    SELECT      s.game_id espn_game_id,
                s.game_ts,
                s.scrape_ts scrape_ts
    FROM        ncaaf_es_scrape s
    INNER JOIN  (
                SELECT s.game_id espn_game_id,
                       MAX(scrape_ts) scrape_ts
                FROM   ncaaf_es_scrape s
                WHERE s.game_ts IS NOT NULL
                GROUP BY 1
                ) fltr
                ON s.scrape_ts = fltr.scrape_ts
                AND s.game_id = fltr.espn_game_id
    WHERE week_id = 2
) es
ON      g.espn_game_id = es.espn_game_id
SET     g.game_ts = es.game_ts,
        g.update_ts = es.scrape_ts
;
