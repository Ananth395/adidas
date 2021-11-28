SELECT title
FROM {parsed_table_name}
WHERE number_of_pages = (
SELECT max(number_of_pages)
FROM {parsed_table_name}
)