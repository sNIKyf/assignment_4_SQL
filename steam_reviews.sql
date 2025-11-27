CREATE OR REPLACE TABLE steam_reviews_complete AS
WITH raw_file AS (
    SELECT *
    FROM read_json_auto(
        '/Users/new/Downloads/steam_2025_5k-dataset-reviews_20250901.json.gz',
        maximum_object_size = 536870912
    )
),
games_unnested AS (
    SELECT UNNEST(reviews) as game_obj
    FROM raw_file
),
reviews_unnested AS (
    SELECT game_obj.appid,
        game_obj.review_data.query_summary as game_stats,
        UNNEST(game_obj.review_data.reviews) as review_obj
    FROM games_unnested
    WHERE game_obj.review_data.success = 1 -- Only keep successful data fetches
)
SELECT appid,
    review_obj.recommendationid as review_id,
    review_obj.language,
    review_obj.review as review_text,
    review_obj.voted_up,
    review_obj.steam_purchase,
    review_obj.received_for_free,
    review_obj.written_during_early_access,
    review_obj.votes_up,
    review_obj.votes_funny,
    review_obj.comment_count,
    CAST(review_obj.weighted_vote_score AS DOUBLE) as weighted_score,
    review_obj.author.steamid as author_id,
    review_obj.author.num_games_owned,
    review_obj.author.num_reviews as author_total_reviews,
    review_obj.author.playtime_forever as playtime_total_minutes,
    review_obj.author.playtime_at_review as playtime_at_review_minutes,
    to_timestamp(review_obj.timestamp_created) as created_at,
    to_timestamp(review_obj.timestamp_updated) as updated_at,
    game_stats.review_score_desc as game_overall_sentiment,
    game_stats.total_positive as game_total_positive,
    game_stats.total_negative as game_total_negative,
    game_stats.review_score as game_score_code

FROM reviews_unnested;

SELECT * FROM steam_reviews_complete LIMIT 20;


-- avg number of reviews by game sentiment
WITH unique_reviews AS(
    SELECT DISTINCT appid,
            game_overall_sentiment,
            game_total_positive,
            game_total_negative
    FROM steam_reviews_complete
)
SELECT game_overall_sentiment,
       AVG(game_total_positive+game_total_negative) AS avg_reviews
FROM unique_reviews
GROUP BY game_overall_sentiment
ORDER BY avg_reviews DESC;



-- The most toxic reviewers by language
WITH negative_language AS (
    SELECT language,
        COUNT(*) as negative_reviews
    FROM steam_reviews_complete
    WHERE voted_up = FALSE
    GROUP BY language
)
SELECT
    r.language,
    n.negative_reviews/COUNT(r.review_id) * 100 as percent_neg_reviews
FROM steam_reviews_complete AS r
JOIN negative_language AS n ON r.language = n.language
GROUP BY r.language, n.negative_reviews
ORDER BY percent_neg_reviews DESC;

