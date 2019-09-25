INSERT INTO ud_spreads (game_id)
SELECT game_id 
FROM ud_games 
WHERE week_id = 5 
ORDER BY by game_id
;
