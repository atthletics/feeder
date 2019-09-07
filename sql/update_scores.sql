UPDATE      ud_games g
INNER JOIN
(
    SELECT      s.game_id espn_game_id,
                s.away_score,
                s.home_score,
                s.scrape_ts scrape_ts
    FROM        ncaaf_es_scrape s
    INNER JOIN  (
                SELECT s.game_id espn_game_id,
                       MAX(scrape_ts) scrape_ts
                FROM   ncaaf_es_scrape s
                WHERE s.away_score IS NOT NULL
                AND   s.home_score IS NOT NULL
                GROUP BY 1
                ) fltr
                ON s.scrape_ts = fltr.scrape_ts
                AND s.game_id = fltr.espn_game_id
    WHERE week_id = 2
) es
ON      g.espn_game_id = es.espn_game_id
SET     g.away_score = es.away_score,
        g.home_score = es.home_score,
        g.is_final = 1,
        g.update_ts = es.scrape_ts
;
