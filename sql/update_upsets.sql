UPDATE      ud_spreads s
INNER JOIN
(
    SELECT      g.game_id,
                sc.scrape_ts
    FROM        ud_games g
    LEFT JOIN  (
                SELECT      s.game_id espn_game_id,
                            s.winner,
                            s.scrape_ts scrape_ts
                FROM        ncaaf_es_scrape s
                INNER JOIN  (
                            SELECT s.game_id espn_game_id,
                                   MAX(scrape_ts) scrape_ts
                            FROM   ncaaf_es_scrape s
                            WHERE s.away_score IS NOT NULL
                            AND   s.home_score IS NOT NULL
                            AND   s.game_status LIKE '%WIN%'
                            GROUP BY 1
                            ) fltr
                            ON s.scrape_ts = fltr.scrape_ts
                            AND s.game_id = fltr.espn_game_id
                ) sc
                ON g.espn_game_id = sc.espn_game_id
    LEFT JOIN   teams win
                ON sc.winner = win.espn_team_id
    INNER JOIN  ud_spreads sp
                ON g.game_id = sp.game_id
                AND win.atthletics_team_id = sp.team_id
    WHERE g.week_id = 1
) ups
ON          s.game_id = ups.game_id
SET         s.is_upset = 1,
            s.create_ts = ups.scrape_ts
;
