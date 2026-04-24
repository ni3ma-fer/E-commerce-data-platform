-- dbt/tests/gold_referential_integrity.sql
-- Retourne des lignes si des FK sont orphelines → test FAIL → DAG bloqué
SELECT f.transaction_id, 'missing_customer' AS integrity_error
FROM {{ ref('fact_orders') }} f
LEFT JOIN {{ ref('dim_customers') }} c ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL
UNION ALL
SELECT f.transaction_id, 'missing_product' AS integrity_error
FROM {{ ref('fact_orders') }} f
LEFT JOIN {{ ref('dim_products') }} p ON f.product_key = p.product_key
WHERE p.product_key IS NULL
