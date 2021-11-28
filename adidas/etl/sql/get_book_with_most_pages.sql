SELECT title
FROM {table_name}
WHERE number_of_pages = (
SELECT max(number_of_pages)
FROM {table_name}
)