SELECT
books.publish_year,
books.publish_month,
no_of_authors,
no_of_books_published
FROM
(SELECT publish_year,
publish_month,
count(*) as no_of_authors
FROM (
SELECT explode(authors.key) as author,
publish_year,
publish_month,
key
FROM {parsed_table_name}
WHERE cast(publish_year as int) between 1950 and 1970
) GROUP BY publish_year, publish_month
) authors
INNER JOIN
(
SELECT publish_year,
publish_month,
count(*) as no_of_books_published
FROM (
SELECT publish_year,
publish_month,
key
FROM {parsed_table_name}
WHERE cast(publish_year as int) between 1950 and 1970)
GROUP BY publish_year, publish_month
) books
ON authors.publish_year=books.publish_year
AND authors.publish_month=books.publish_month
ORDER BY books.publish_year, books.publish_month