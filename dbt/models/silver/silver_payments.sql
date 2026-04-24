-- =============================================================================
-- dbt/models/silver/silver_payments.sql
-- KiVendTout — Couche Silver · Paiements
-- =============================================================================
--
-- REFACTORISATIONS APPLIQUÉES (v2) :
-- ─────────────────────────────────────────────────────────────────────────────
-- [1] WINDOW FUNCTIONS SQL : Les features ML (user_txn_count_30d, etc.) ne
--     sont plus lues depuis la source. Elles sont calculées directement ici
--     via des fonctions fenêtrées RANGE BETWEEN INTERVAL 30 DAYS PRECEDING.
--     → Supprime la dépendance au job Spark compute_behavioral_features().
--
-- [2] STYLE CTE STANDARD dbt : source_data → deduplicated → cleaned →
--     with_features → final. Chaque CTE a une responsabilité unique.
--     → Lisibilité maximale, testabilité, dbt docs génère le lineage complet.
--
-- [3] FILTRE INCRÉMENTAL CORRIGÉ : la clause WHERE utilise ingested_at
--     (timestamp d'ingestion Bronze) et non silver_loaded_at pour éviter
--     les doublons lors des re-runs. Le lookback est géré par le MERGE.
-- =============================================================================

{{
  config(
    materialized      = 'incremental',
    unique_key        = 'transaction_id',
    incremental_strategy = 'merge',
    on_schema_change  = 'sync_all_columns',
    cluster_by        = ['event_date'],
    tags              = ['silver', 'payments', 'daily', 'ml-features']
  )
}}

-- =============================================================================
-- CTE 1 : source_data
-- Lecture de la Bronze avec filtre incrémental.
-- On lit uniquement les enregistrements non encore traités en Silver.
-- NOTE : pas de lookback ici car les paiements sont atomiques (pas de
--        sessions à cheval sur minuit comme le clickstream).
-- =============================================================================
WITH source_data AS (

    SELECT *
    FROM {{ source('bronze', 'payments') }}

    {% if is_incremental() %}
    -- Filtre incrémental : ingested_at (timestamp Bronze) > dernière Silver
    -- Utilisation de ingested_at et non silver_loaded_at pour éviter de
    -- rater les messages arrivés en retard dans Kafka (jusqu'à 7j de buffer).
    WHERE ingested_at > (
        SELECT COALESCE(MAX(ingested_at), '1970-01-01'::TIMESTAMP)
        FROM {{ this }}
    )
    {% endif %}

),

-- =============================================================================
-- CTE 2 : deduplicated
-- Déduplication sur transaction_id : on conserve la version la plus récente
-- (ingested_at DESC) pour gérer les retries du producteur Kafka.
-- En cas de re-delivery Kafka, l'event le plus récent est la référence.
-- =============================================================================
deduplicated AS (

    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY ingested_at DESC
            ) AS _row_num
        FROM source_data
        WHERE transaction_id IS NOT NULL   -- Rejet précoce des nulls primaires
    ) ranked
    WHERE _row_num = 1

),

-- =============================================================================
-- CTE 3 : cleaned
-- Nettoyage et typage : normalisation des types, standardisation des codes,
-- calcul des flags métier simples.
-- Les colonnes techniques Bronze (_raw_json, _parse_error, kafka_*) sont
-- exclues du SELECT final pour ne pas polluer la Silver.
-- =============================================================================
cleaned AS (

    SELECT
        -- ── Clés ───────────────────────────────────────────────────────────
        transaction_id,
        user_id,
        merchant_id,
        order_id,

        -- ── Montant : arrondi 2 décimales, valeur absolue (Bronze brut) ───
        ROUND(ABS(CAST(amount AS DECIMAL(12, 2))), 2)           AS amount_eur,

        -- ── Devise : majuscules, défaut EUR si NULL ─────────────────────────
        COALESCE(UPPER(TRIM(currency)), 'EUR')                  AS currency_code,

        -- ── Timestamp : cast depuis le string ISO 8601 Bronze ───────────────
        CAST(timestamp AS TIMESTAMP)                            AS event_timestamp,
        CAST(CAST(timestamp AS TIMESTAMP) AS DATE)              AS event_date,

        -- ── Dérivés temporels ───────────────────────────────────────────────
        HOUR(CAST(timestamp AS TIMESTAMP))                      AS event_hour,
        DAYOFWEEK(CAST(timestamp AS TIMESTAMP))                 AS event_day_of_week,
        CASE
            WHEN HOUR(CAST(timestamp AS TIMESTAMP)) BETWEEN 0  AND 6  THEN 'night'
            WHEN HOUR(CAST(timestamp AS TIMESTAMP)) BETWEEN 7  AND 12 THEN 'morning'
            WHEN HOUR(CAST(timestamp AS TIMESTAMP)) BETWEEN 13 AND 18 THEN 'afternoon'
            ELSE 'evening'
        END                                                     AS time_of_day,

        -- ── Géographie : ISO 3166 majuscules ───────────────────────────────
        UPPER(TRIM(billing_country))                            AS billing_country_iso,
        UPPER(TRIM(shipping_country))                           AS shipping_country_iso,
        UPPER(TRIM(ip_country_code))                            AS ip_country_code,

        -- ── Moyens de paiement ──────────────────────────────────────────────
        payment_method,
        CAST(is_new_card    AS BOOLEAN)                         AS is_new_card,
        CAST(is_new_merchant AS BOOLEAN)                        AS is_new_merchant,

        -- ── KYC / Vérification identité ────────────────────────────────────
        user_kyc_status,
        CAST(user_is_minor AS BOOLEAN)                          AS user_is_minor,
        CAST(user_age AS INT)                                   AS user_age,

        -- ── Fraude (ground truth ML) ────────────────────────────────────────
        fraud_type,
        CAST(fraud_score_label AS DOUBLE)                       AS fraud_score_label,
        CAST(blocked AS BOOLEAN)                                AS blocked,
        reject_reason,
        CAST(geo_distance_km AS DOUBLE)                         AS geo_distance_km,
        CAST(velocity_count_5min AS INT)                        AS velocity_count_5min,

        -- ── Métadonnées d'ingestion ─────────────────────────────────────────
        ingested_at,
        pii_anonymized_at,

        -- ── Colonnes techniques passées pour les CTEs suivantes ─────────────
        ip_address,
        device_fingerprint

    FROM deduplicated

    -- Filtre qualité Silver : rejette les lignes structurellement invalides.
    -- Les lignes rejetées sont gérées par le job Spark → quarantaine Bronze.
    WHERE amount > 0
      AND user_id IS NOT NULL
      AND currency IS NOT NULL

),

-- =============================================================================
-- CTE 4 : with_cross_border
-- Flag cross-border calculé après normalisation des pays.
-- Séparé de cleaned pour que les colonnes normalisées soient disponibles.
-- =============================================================================
with_cross_border AS (

    SELECT
        *,
        -- Cross-border : pays de facturation ≠ pays de livraison
        (billing_country_iso != shipping_country_iso)           AS is_cross_border,
        -- IP géo-incohérente : IP dans un pays différent du pays de facturation
        (ip_country_code != billing_country_iso)                AS ip_geo_mismatch

    FROM cleaned

),

-- =============================================================================
-- CTE 5 : with_ml_features
-- ─────────────────────────────────────────────────────────────────────────────
-- REFACTORISATION #1 : Calcul des features ML via SQL Window Functions.
-- Ces features remplacent les colonnes précédemment calculées par le job Spark
-- compute_behavioral_features() et attendues depuis la source.
--
-- POURQUOI SQL PLUTÔT QUE SPARK ICI :
--   • Databricks SQL exécute les window functions en mode vectorisé (Photon)
--   • Le calcul est reproductible et testé via dbt test
--   • La fenêtre 30j est cohérente avec celle du Feature Store Gold
--   • Pas de dérive training/serving (même SQL dans Gold fraud_feature_store)
--
-- FENÊTRE TEMPORELLE :
--   RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW
--   = fenêtre glissante de 30 jours basée sur event_timestamp
--   Compatible Databricks SQL / Spark SQL (pas de standard ANSI strict requis)
-- =============================================================================
with_ml_features AS (

    SELECT
        *,

        -- ── FEATURE 1 : Nombre de transactions (30 jours glissants) ─────────
        -- Signal de vélocité : un count élevé en peu de temps = carding suspect
        COUNT(transaction_id) OVER (
            PARTITION BY user_id
            ORDER BY CAST(event_timestamp AS LONG)
            RANGE BETWEEN (30 * 86400) PRECEDING AND CURRENT ROW
        )                                                       AS user_txn_count_30d,

        -- ── FEATURE 2 : Montant moyen sur 30 jours ─────────────────────────
        -- Baseline comportementale : un écart important = signal d'anomalie
        AVG(amount_eur) OVER (
            PARTITION BY user_id
            ORDER BY CAST(event_timestamp AS LONG)
            RANGE BETWEEN (30 * 86400) PRECEDING AND CURRENT ROW
        )                                                       AS user_avg_amount_30d,

        -- ── FEATURE 3 : Écart-type du montant sur 30 jours ─────────────────
        -- Mesure la volatilité comportementale de l'utilisateur
        STDDEV(amount_eur) OVER (
            PARTITION BY user_id
            ORDER BY CAST(event_timestamp AS LONG)
            RANGE BETWEEN (30 * 86400) PRECEDING AND CURRENT ROW
        )                                                       AS user_amount_stddev_30d,

        -- ── FEATURE 4 : Ratio montant courant / moyenne 30j ────────────────
        -- > 3.0 = transaction anormalement élevée → feature XGBoost critique
        -- NULLIF évite la division par zéro pour les nouveaux utilisateurs
        ROUND(
            amount_eur / NULLIF(
                AVG(amount_eur) OVER (
                    PARTITION BY user_id
                    ORDER BY CAST(event_timestamp AS LONG)
                    RANGE BETWEEN (30 * 86400) PRECEDING AND CURRENT ROW
                ),
                0
            ),
            4
        )                                                       AS amount_vs_avg_ratio,

        -- ── FEATURE 5 : Flag premier achat (toute l'histoire) ───────────────
        -- Comptes récents + gros montants = pattern à risque élevé
        (
            ROW_NUMBER() OVER (
                PARTITION BY user_id
                ORDER BY event_timestamp ASC
            ) = 1
        )                                                       AS is_first_transaction,

        -- ── FEATURE 6 : Nombre de pays distincts utilisés (30j) ─────────────
        -- Plusieurs pays en 30j = itinérance ou fraude multi-pays
        COUNT(DISTINCT billing_country_iso) OVER (
            PARTITION BY user_id
            ORDER BY CAST(event_timestamp AS LONG)
            RANGE BETWEEN (30 * 86400) PRECEDING AND CURRENT ROW
        )                                                       AS user_distinct_countries_30d,

        -- ── FEATURE 7 : Nombre de marchands distincts (30j) ─────────────────
        COUNT(DISTINCT merchant_id) OVER (
            PARTITION BY user_id
            ORDER BY CAST(event_timestamp AS LONG)
            RANGE BETWEEN (30 * 86400) PRECEDING AND CURRENT ROW
        )                                                       AS user_distinct_merchants_30d

    FROM with_cross_border

),

-- =============================================================================
-- CTE 6 : final
-- Assemblage final : sélection propre des colonnes exportées en Silver.
-- Les colonnes techniques intermédiaires (_row_num, ip_address brute, etc.)
-- sont exclues pour ne pas exposer des données inutiles ou sensibles.
-- silver_loaded_at est ajouté ici pour piloter le filtre incrémental
-- du prochain run (via MAX(silver_loaded_at) dans la CTE source_data).
-- =============================================================================
final AS (

    SELECT
        -- ── Clés ────────────────────────────────────────────────────────────
        transaction_id,
        user_id,
        merchant_id,
        order_id,

        -- ── Données financières ──────────────────────────────────────────────
        amount_eur,
        currency_code,

        -- ── Temporalité ─────────────────────────────────────────────────────
        event_timestamp,
        event_date,
        event_hour,
        event_day_of_week,
        time_of_day,

        -- ── Géographie ──────────────────────────────────────────────────────
        billing_country_iso,
        shipping_country_iso,
        ip_country_code,
        is_cross_border,
        ip_geo_mismatch,
        geo_distance_km,

        -- ── Paiement ────────────────────────────────────────────────────────
        payment_method,
        is_new_card,
        is_new_merchant,

        -- ── KYC ─────────────────────────────────────────────────────────────
        user_kyc_status,
        user_is_minor,
        user_age,

        -- ── Features ML (window functions) ──────────────────────────────────
        user_txn_count_30d,
        user_avg_amount_30d,
        user_amount_stddev_30d,
        amount_vs_avg_ratio,
        is_first_transaction,
        user_distinct_countries_30d,
        user_distinct_merchants_30d,
        velocity_count_5min,

        -- ── Fraude ──────────────────────────────────────────────────────────
        fraud_type,
        fraud_score_label,
        blocked,
        reject_reason,

        -- ── Audit pipeline ───────────────────────────────────────────────────
        ingested_at,
        pii_anonymized_at,
        CURRENT_TIMESTAMP()                                     AS silver_loaded_at

    FROM with_ml_features

)

-- Point d'entrée dbt
SELECT * FROM final