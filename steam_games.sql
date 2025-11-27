CREATE OR REPLACE TABLE steam_games_complete AS
WITH raw_file AS (
    SELECT *
    FROM read_json_auto(
        '/Users/new/Downloads/steam_2025_5k-dataset-games_20250831.json.gz',
        maximum_object_size = 536870912
    )
),
unnested_games AS (
    SELECT UNNEST(games) as game_wrapper
    FROM raw_file
),
flattened_data AS (
    SELECT game_wrapper.app_details.data AS info
    FROM unnested_games
    WHERE game_wrapper.app_details.success = true
      AND game_wrapper.app_details.data IS NOT NULL
)
SELECT info.steam_appid as appid,
    info.name,
    info.short_description,
    info.required_age,
    info.website,
    info.release_date.date as release_date,
    info.is_free,
    info.price_overview.final_formatted as price,
    info.content_descriptors.notes as content_warning,
    info.achievements.total as achievements_count,
    info.metacritic.score as metacritic_score,
    info.recommendations.total as user_reviews_count,
    regexp_replace(regexp_replace(info.pc_requirements.minimum, '<br>', '\n', 'g'), '<[^>]+>', '', 'g') as pc_req_min,
    regexp_replace(regexp_replace(info.pc_requirements.recommended, '<br>', '\n', 'g'), '<[^>]+>', '', 'g') as pc_req_rec,
    array_to_string(info.developers, ', ') as developers,
    array_to_string(info.publishers, ', ') as publishers,
    array_to_string((SELECT list(x->>'description') FROM UNNEST(info.genres) AS t(x)), ', ') as genres,
    array_to_string((SELECT list(x->>'description') FROM UNNEST(info.categories) AS t(x)), ', ') as categories,
    info.platforms

FROM flattened_data;


SELECT * FROM steam_games_complete LIMIT 100;

-- Top 20 games by reviews
SELECT name,
       user_reviews_count
FROM steam_games_complete
WHERE user_reviews_count IS NOT NULL
ORDER BY user_reviews_count DESC
LIMIT 20;

-- Distribution of game release years

SELECT
    YEAR(try_strptime(release_date, '%b %d, %Y')) as year,
    COUNT(*) as game_count
FROM steam_games_complete
WHERE year IS NOT NULL
GROUP BY year
ORDER BY year DESC;

-- Top ten free games with the highest metacritic score
SELECT * FROM steam_games_complete
WHERE metacritic_score IS NOT NULL and is_free = TRUE
ORDER BY metacritic_score DESC
LIMIT 10
