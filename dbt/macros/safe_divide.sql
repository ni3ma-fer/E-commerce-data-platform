-- dbt/macros/safe_divide.sql
{% macro safe_divide(numerator, denominator) %}
    CASE
        WHEN {{ denominator }} = 0 OR {{ denominator }} IS NULL THEN NULL
        ELSE ROUND(CAST({{ numerator }} AS DOUBLE) / {{ denominator }}, 4)
    END
{% endmacro %}
