SELECT publish_year,
count(*) as no_of_authors_with_one_book
FROM (
SELECT
explode(authors.key) as author,
publish_year
FROM {parsed_table_name}
)
GROUP BY publish_year
HAVING count(*) > 1