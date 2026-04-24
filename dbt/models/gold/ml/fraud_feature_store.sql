# dbt/models/gold/ml/fraud_feature_store.sql
-- dbt/models/gold/ml/fraud_feature_store.sql
-- Feature Store : features pré-calculées pour XGBoost.
-- 1 ligne = 1 transaction avec toutes ses features à l'instant T.
{{ config(materialized='table', tags=['gold','ml','feature-store','xgboost']) }}
 
WITH txn_features AS (
    SELECT
        user_id, event_date AS feature_date,
        -- Features vélocité (fenêtres glissantes)
        COUNT(*) OVER (
            PARTITION BY user_id ORDER BY event_timestamp
            RANGE BETWEEN INTERVAL 5 MINUTES PRECEDING AND CURRENT ROW
        )                                       AS txn_count_5min,
        COUNT(*) OVER (
            PARTITION BY user_id ORDER BY event_timestamp
            RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW
        )                                       AS txn_count_1h,
        COUNT(*) OVER (
            PARTITION BY user_id ORDER BY CAST(event_date AS TIMESTAMP)
            RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW
        )                                       AS txn_count_30d,
        -- Features montant
        AVG(amount_eur) OVER (
            PARTITION BY user_id ORDER BY CAST(event_date AS TIMESTAMP)
            RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW
        )                                       AS avg_amount_30d,
        amount_eur / NULLIF(AVG(amount_eur) OVER (
            PARTITION BY user_id ORDER BY CAST(event_date AS TIMESTAMP)
            RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW
        ), 0)                                   AS amount_vs_avg_ratio,
        -- Features géographiques et KYC
        is_cross_border, geo_distance_km, kyc_status, age_group, is_minor,
        -- Features comportementales
        is_first_transaction, is_new_card, is_new_merchant,
        -- Features produit
        risk_category, category_id,
        -- Label ground truth (cible de XGBoost)
        fraud_label                             AS is_fraud,
        fraud_score_label,
        fraud_type,
        transaction_id,
        CURRENT_TIMESTAMP()                     AS feature_created_at
    FROM {{ ref('fact_orders') }}
)
 
SELECT * FROM txn_features
WHERE is_fraud IS NOT NULL
