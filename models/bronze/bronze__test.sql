{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

SELECT
    ROW_NUMBER() over (
        ORDER BY
            SEQ4()
    ) AS id
FROM
    TABLE(GENERATOR(rowcount => 1000))
