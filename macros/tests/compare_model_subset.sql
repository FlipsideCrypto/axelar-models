{% test compare_model_subset(
    model,
    compare_model,
    compare_columns,
    model_condition
) %}
{% set compare_cols_csv = compare_columns | join(', ') %}
WITH A AS (
    SELECT
        {{ compare_cols_csv }}
    FROM
        {{ model }}
        {{ model_condition }}
),
b AS (
    SELECT
        {{ compare_cols_csv }}
    FROM
        {{ compare_model }}
),
a_minus_b AS (
    SELECT
        *
    FROM
        A {{ dbt_utils.except() }}
    SELECT
        *
    FROM
        b
),
b_minus_a AS (
    SELECT
        *
    FROM
        b {{ dbt_utils.except() }}
    SELECT
        *
    FROM
        A
),
unioned AS (
    SELECT
        'in_actual_not_in_expected' AS which_diff,
        a_minus_b.*
    FROM
        a_minus_b
    UNION ALL
    SELECT
        'in_expected_not_in_actual' AS which_diff,
        b_minus_a.*
    FROM
        b_minus_a
)
SELECT
    *
FROM
    unioned {% endtest %}
