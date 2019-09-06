SELECT      g.game_id,
            sc.away_score,
            sc.home_score,
            away.os_team_name away_team,
            home.os_team_name home_team,
            win.os_team_name winner,
            sc.scrape_ts
FROM        ud_games g
LEFT JOIN  (
            SELECT      s.game_id espn_game_id,
                        s.away_score,
                        s.home_score,
                        s.winner,
                        s.scrape_ts scrape_ts
            FROM        ncaaf_es_scrape s
            INNER JOIN  (
                        SELECT s.game_id espn_game_id,
                               MAX(scrape_ts) scrape_ts
                        FROM   ncaaf_es_scrape s
                        WHERE s.away_score IS NOT NULL
                        GROUP BY 1
                        ) fltr
                        ON s.scrape_ts = fltr.scrape_ts
                        AND s.game_id = fltr.espn_game_id
            ) sc
            ON g.espn_game_id = sc.espn_game_id
LEFT JOIN   teams win
            ON sc.winner = win.espn_team_id
LEFT JOIN   teams away
            ON g.away_id = away.atthletics_team_id
LEFT JOIN   teams home
            ON g.home_id = home.atthletics_team_id
INNER JOIN  ud_spreads sp
            ON g.game_id = sp.game_id
            AND win.atthletics_team_id = sp.team_id
WHERE g.week_id = 1
;
