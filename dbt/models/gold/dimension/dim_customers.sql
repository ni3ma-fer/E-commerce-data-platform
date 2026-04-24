# dbt/models/gold/dimensions/dim_customers.sql
-- dbt/models/gold/dimensions/dim_customers.sql
-- Dimension clients — hub central du star schema.
-- Source : silver_crm (PII déjà pseudonymisées par Presidio)
-- Aucune PII en Gold — conformité RGPD garantie par la Silver.
 
{{
  config(
    materialized='table',
    tags=['gold', 'dimension', 'customers'],
    cluster_by=['customer_segment']
  )
}}
 
WITH crm AS (SELECT * FROM {{ source('silver', 'silver_crm') }}),
 
payments_agg AS (
    SELECT
        user_id,
        COUNT(DISTINCT transaction_id)          AS lifetime_orders,
        SUM(amount_eur)                         AS lifetime_value_eur,
        AVG(amount_eur)                         AS avg_order_value_eur,
        MAX(event_date)                         AS last_purchase_date,
        MIN(event_date)                         AS first_purchase_date,
        SUM(CASE WHEN fraud_label THEN 1 ELSE 0 END) AS fraud_attempts
    FROM {{ source('silver', 'silver_payments') }}
    WHERE status = 'success'
    GROUP BY user_id
),
 
sessions_agg AS (
    SELECT user_id,
        COUNT(DISTINCT session_id)              AS total_sessions,
        AVG(session_duration_seconds)           AS avg_session_duration_s,
        AVG(CASE WHEN has_conversion THEN 1.0 ELSE 0.0 END) AS conversion_rate
    FROM {{ source('silver', 'silver_sessions') }}
    GROUP BY user_id
)
 
SELECT
    {{ dbt_utils.generate_surrogate_key(['crm.user_id']) }} AS customer_key,
    crm.user_id,
    crm.age,
    CASE
        WHEN crm.age < 18 THEN 'minor'
        WHEN crm.age < 25 THEN '18-24'
        WHEN crm.age < 35 THEN '25-34'
        WHEN crm.age < 50 THEN '35-49'
        ELSE '50+'
    END                                     AS age_group,
    crm.city,
    crm.postal_zone,          -- Code postal tronqué (4 chiffres) — RGPD
    crm.country,
    crm.id_verification_status              AS kyc_status,
    crm.is_minor,
    (crm.is_minor AND crm.id_verification_status = 'verified') AS kyc_minor_anomaly,
    crm.customer_segment,
    -- Métriques RFM
    DATEDIFF(CURRENT_DATE(), p.last_purchase_date)  AS recency_days,
    COALESCE(p.lifetime_orders, 0)                  AS frequency,
    COALESCE(p.lifetime_value_eur, 0.0)             AS monetary_eur,
    COALESCE(p.avg_order_value_eur, 0.0)            AS avg_order_value_eur,
    NTILE(5) OVER (ORDER BY DATEDIFF(CURRENT_DATE(), p.last_purchase_date) DESC) AS rfm_recency_score,
    NTILE(5) OVER (ORDER BY COALESCE(p.lifetime_orders, 0))  AS rfm_frequency_score,
    NTILE(5) OVER (ORDER BY COALESCE(p.lifetime_value_eur, 0)) AS rfm_monetary_score,
    COALESCE(s.total_sessions, 0)           AS total_sessions,
    COALESCE(s.conversion_rate, 0.0)        AS session_conversion_rate,
    COALESCE(p.fraud_attempts, 0)           AS fraud_attempts_count,
    (COALESCE(p.fraud_attempts, 0) > 0)     AS has_fraud_history,
    crm.registration_date,
    p.first_purchase_date,
    p.last_purchase_date,
    crm.gdpr_consent_date,
    crm.newsletter_optin,
    CURRENT_TIMESTAMP()                     AS gold_loaded_at
FROM crm
LEFT JOIN payments_agg  p ON crm.user_id = p.user_id
LEFT JOIN sessions_agg  s ON crm.user_id = s.user_id
