SELECT substring(co_authors,10) as top_5_co_authors
FROM(
SELECT co_authors, 
count(*) as cnt 
FROM
(
SELECT explode(authors.key) as co_authors
FROM {table_name} 
WHERE key in (
SELECT key 
FROM (
SELECT explode(authors.key) as authors,
key 
FROM {table_name}
)
GROUP BY key
HAVING count(*) > 1
)
)
GROUP BY co_authors
ORDER BY cnt desc
) limit 5