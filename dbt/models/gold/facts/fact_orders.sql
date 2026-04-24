# dbt/models/gold/facts/fact_orders.sql
-- dbt/models/gold/facts/fact_orders.sql
-- Table de faits : 1 ligne = 1 transaction réussie.
{{
  config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    cluster_by=['event_date'],
    tags=['gold', 'fact', 'orders']
  )
}}
 
WITH payments AS (
    SELECT * FROM {{ source('silver', 'silver_payments') }}
    WHERE status = 'success'
    {% if is_incremental() %}
    AND silver_loaded_at > (
        SELECT COALESCE(MAX(gold_loaded_at), '1970-01-01') FROM {{ this }}
    )
    {% endif %}
)
 
SELECT
    {{ dbt_utils.generate_surrogate_key(['p.transaction_id']) }} AS fact_key,
    p.transaction_id,
    p.order_id,
    c.customer_key,
    pr.product_key,
    d.date_key                              AS order_date_key,
    p.amount_eur,
    p.currency_code,
    p.payment_method,
    p.quantity,
    p.billing_country_iso,
    p.shipping_country_iso,
    p.is_cross_border,
    p.is_first_transaction,
    p.is_new_card,
    p.user_txn_count_30d,
    p.amount_vs_avg_ratio,
    p.fraud_label,
    p.fraud_score_label,
    p.fraud_type,
    c.customer_segment,
    c.kyc_status,
    c.age_group,
    pr.category_id,
    pr.risk_category,
    p.event_date,
    p.event_timestamp,
    CURRENT_TIMESTAMP()                     AS gold_loaded_at
FROM payments p
LEFT JOIN {{ ref('dim_customers') }} c  ON p.user_id    = c.user_id
LEFT JOIN {{ ref('dim_products')  }} pr ON p.product_id = pr.product_id
LEFT JOIN {{ ref('dim_date')      }} d  ON p.event_date = d.date_actual
