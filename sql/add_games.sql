INSERT INTO ud_spreads (game_id)
SELECT game_id 
FROM ud_games 
WHERE week_id = 6
ORDER BY game_id
;
