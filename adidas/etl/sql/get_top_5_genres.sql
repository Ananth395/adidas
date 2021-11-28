SELECT exploded_genres as top_5_genres
FROM (
SELECT exploded_genres,
count(*) as cnt
FROM (
SELECT explode(genres) as exploded_genres
FROM {table_name}
WHERE genres is not NULL )
GROUP BY exploded_genres
ORDER BY cnt desc
limit 5
)