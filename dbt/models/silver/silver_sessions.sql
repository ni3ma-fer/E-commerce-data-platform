-- =============================================================================
-- dbt/models/silver/silver_sessions.sql
-- KiVendTout — Couche Silver · Sessions Clickstream
-- =============================================================================
--
-- REFACTORISATIONS APPLIQUÉES (v2) :
-- ─────────────────────────────────────────────────────────────────────────────
-- [2] STYLE CTE STANDARD dbt : source_data → session_boundaries →
--     session_metrics → session_pages → final.
--
-- [3] LOOKBACK WINDOW 3 JOURS : Le filtre incrémental v1 utilisait
--     ingested_at > MAX(silver_loaded_at), ce qui coupait les sessions
--     à cheval sur minuit (une session qui commence à 23h55 et se termine
--     à 00h05 n'était pas reconstituée correctement au run du lendemain).
--
--     SOLUTION : lookback de 3 jours sur event_timestamp.
--     On relit systématiquement les 3 derniers jours de Bronze clickstream
--     et on laisse le MERGE (unique_key = session_id) déduplicater.
--
--     IMPACT PERFORMANCE : +3j de données Bronze relues à chaque run.
--     En pratique : clickstream ~10M events/jour → 30M lignes Bronze scannées.
--     Sur Databricks avec partition pruning (year/month/day), scan partiel
--     → acceptable pour une Silver journalière.
--
--     ALTERNATIVE SI TROP LENT : réduire à 1j de lookback et ajouter une
--     macro dbt `get_lookback_days()` configurable par environnement.
-- =============================================================================

{{
  config(
    materialized         = 'incremental',
    unique_key           = 'session_id',
    incremental_strategy = 'merge',
    on_schema_change     = 'sync_all_columns',
    cluster_by           = ['session_date'],
    tags                 = ['silver', 'sessions', 'daily', 'clickstream']
  )
}}

-- =============================================================================
-- CTE 1 : source_data
-- Lecture Bronze clickstream avec lookback 3 jours.
--
-- POURQUOI event_timestamp ET NON ingested_at POUR LE LOOKBACK :
--   • Un event peut être ingéré en Bronze avec plusieurs heures de retard
--     (Kafka buffer, retries producteur). Filtrer sur ingested_at raterait
--     ces events tardifs qui appartiennent à des sessions d'hier.
--   • event_timestamp = moment réel de l'action utilisateur → source fiable.
--   • Le MERGE sur session_id garantit l'idempotence même si on relit
--     des events déjà traités dans un run précédent.
-- =============================================================================
WITH source_data AS (

    SELECT *
    FROM {{ source('bronze', 'clickstream') }}

    {% if is_incremental() %}
    -- LOOKBACK 3 JOURS sur event_timestamp (pas sur ingested_at).
    -- Capte les sessions à cheval sur minuit et les events Kafka en retard.
    -- Le MERGE sur session_id empêche les doublons de sessions déjà traitées.
    WHERE CAST(event_timestamp AS TIMESTAMP) >= CURRENT_DATE() - INTERVAL 3 DAYS

    {% else %}
    -- Full refresh : pas de filtre → traitement complet de l'historique Bronze.
    -- À utiliser lors du premier run ou d'une reconstruction complète.
    -- Commande : dbt run --full-refresh --select silver_sessions

    {% endif %}

),

-- =============================================================================
-- CTE 2 : cleaned_events
-- Nettoyage des événements individuels avant agrégation en sessions.
-- Exclut les bots (duration < 2s) et les events sans session_id.
-- =============================================================================
cleaned_events AS (

    SELECT
        event_id,
        session_id,
        user_id,
        CAST(event_timestamp AS TIMESTAMP)              AS event_timestamp,
        event_type,
        page,

        -- Normalisation device
        CASE
            WHEN LOWER(device_type) LIKE 'mobile%'  THEN 'mobile'
            WHEN LOWER(device_type) LIKE 'tablet%'  THEN 'tablet'
            ELSE 'desktop'
        END                                             AS device_category,

        -- Normalisation navigateur
        CASE
            WHEN LOWER(user_agent) LIKE '%chrome%'  THEN 'chrome'
            WHEN LOWER(user_agent) LIKE '%firefox%' THEN 'firefox'
            WHEN LOWER(user_agent) LIKE '%safari%'  THEN 'safari'
            WHEN LOWER(user_agent) LIKE '%edge%'    THEN 'edge'
            ELSE 'other'
        END                                             AS browser,

        -- Catégorisation source de trafic
        CASE
            WHEN referrer LIKE '%google%'   THEN 'organic_search'
            WHEN referrer LIKE '%bing%'     THEN 'organic_search'
            WHEN referrer LIKE '%facebook%' THEN 'social'
            WHEN referrer LIKE '%instagram%'THEN 'social'
            WHEN referrer LIKE '%tiktok%'   THEN 'social'
            WHEN referrer = 'email_campaign'THEN 'email'
            WHEN referrer = 'direct'
              OR referrer IS NULL           THEN 'direct'
            ELSE 'referral'
        END                                             AS traffic_source,

        CAST(scroll_depth_pct  AS INT)                  AS scroll_depth_pct,
        CAST(duration_ms / 1000.0 AS DOUBLE)            AS duration_seconds,
        product_id,
        category_id,
        CAST(is_adult_product AS BOOLEAN)               AS is_adult_product,
        CAST(cart_value_eur AS DOUBLE)                  AS cart_value_eur,
        ingested_at

    FROM source_data
    WHERE session_id IS NOT NULL
      AND event_id   IS NOT NULL
      -- Filtre bots : durée < 2s = session trop courte, signe de bot
      AND CAST(duration_ms AS INT) >= 2000

),

-- =============================================================================
-- CTE 3 : session_boundaries
-- Calcul des bornes temporelles et métriques de base par session.
-- Agrégation GROUP BY session_id → 1 ligne par session.
-- =============================================================================
session_boundaries AS (

    SELECT
        session_id,
        user_id,
        -- Prend la valeur la plus fréquente (approximation par MAX/MIN)
        MAX(device_category)                            AS device_category,
        MAX(browser)                                    AS browser,
        MAX(traffic_source)                             AS traffic_source,

        -- Bornes temporelles
        MIN(event_timestamp)                            AS session_start,
        MAX(event_timestamp)                            AS session_end,

        -- Durée session en secondes (différence entre premier et dernier event)
        UNIX_TIMESTAMP(MAX(event_timestamp))
        - UNIX_TIMESTAMP(MIN(event_timestamp))          AS session_duration_seconds,

        -- Date de la session (basée sur le premier event)
        CAST(MIN(event_timestamp) AS DATE)              AS session_date,

        -- Métriques d'engagement
        COUNT(*)                                        AS total_events,
        COUNT(CASE WHEN event_type = 'page_view'        THEN 1 END) AS page_views,
        COUNT(CASE WHEN event_type = 'product_view'     THEN 1 END) AS product_views,
        COUNT(CASE WHEN event_type = 'add_to_cart'      THEN 1 END) AS add_to_cart_count,
        COUNT(CASE WHEN event_type = 'remove_from_cart' THEN 1 END) AS remove_from_cart_count,
        COUNT(CASE WHEN event_type = 'checkout_start'   THEN 1 END) AS checkout_starts,
        COUNT(CASE WHEN event_type = 'wishlist_add'     THEN 1 END) AS wishlist_adds,
        COUNT(DISTINCT product_id)                      AS unique_products_viewed,
        COUNT(DISTINCT category_id)                     AS unique_categories_viewed,

        -- Métriques scroll / durée
        MAX(scroll_depth_pct)                           AS max_scroll_depth,
        AVG(scroll_depth_pct)                           AS avg_scroll_depth,
        AVG(duration_seconds)                           AS avg_page_duration_seconds,

        -- Panier
        MAX(cart_value_eur)                             AS max_cart_value_eur,

        -- Conversion : la session a-t-elle abouti à un checkout ?
        MAX(CASE WHEN event_type = 'checkout_start' THEN 1 ELSE 0 END) = 1
                                                        AS has_conversion,

        -- KYC adultes : la session a-t-elle impliqué des produits adultes ?
        MAX(CASE WHEN is_adult_product = TRUE THEN 1 ELSE 0 END) = 1
                                                        AS contains_adult_products,

        -- Timestamp ingestion pour audit
        MAX(ingested_at)                                AS last_event_ingested_at

    FROM cleaned_events
    GROUP BY
        session_id,
        user_id

),

-- =============================================================================
-- CTE 4 : session_pages
-- Calcul de la page d'entrée et de sortie via des window functions.
-- Séparé de session_boundaries car FIRST_VALUE/LAST_VALUE nécessitent
-- une window sur les events individuels, incompatible avec le GROUP BY.
-- =============================================================================
session_pages AS (

    SELECT DISTINCT
        session_id,

        -- Page d'entrée = premier event de la session (ORDER BY event_timestamp ASC)
        FIRST_VALUE(page) OVER (
            PARTITION BY session_id
            ORDER BY event_timestamp ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )                                               AS entry_page,

        -- Page de sortie = dernier event de la session
        LAST_VALUE(page) OVER (
            PARTITION BY session_id
            ORDER BY event_timestamp ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )                                               AS exit_page

    FROM cleaned_events

),

-- =============================================================================
-- CTE 5 : final
-- Jointure session_boundaries + session_pages + calculs dérivés.
-- Filtre de qualité final : sessions trop courtes (< 2s) ou sans events
-- réels sont exclues (peuvent subsister après le filtre cleaned_events
-- si toute la session ne contient que des bots).
-- =============================================================================
final AS (

    SELECT
        -- ── Clés ────────────────────────────────────────────────────────────
        sb.session_id,
        sb.user_id,

        -- ── Contexte technique ───────────────────────────────────────────────
        sb.device_category,
        sb.browser,
        sb.traffic_source,

        -- ── Temporalité ─────────────────────────────────────────────────────
        sb.session_start,
        sb.session_end,
        sb.session_date,
        sb.session_duration_seconds,

        -- ── Métriques engagement ─────────────────────────────────────────────
        sb.total_events,
        sb.page_views,
        sb.product_views,
        sb.add_to_cart_count,
        sb.remove_from_cart_count,
        sb.checkout_starts,
        sb.wishlist_adds,
        sb.unique_products_viewed,
        sb.unique_categories_viewed,
        sb.max_scroll_depth,
        sb.avg_scroll_depth,
        sb.avg_page_duration_seconds,
        sb.max_cart_value_eur,

        -- ── Conversion ──────────────────────────────────────────────────────
        sb.has_conversion,

        -- ── Pages entrée / sortie (depuis session_pages) ─────────────────────
        sp.entry_page,
        sp.exit_page,

        -- ── KYC adultes ─────────────────────────────────────────────────────
        sb.contains_adult_products,

        -- ── Score d'engagement calculé ──────────────────────────────────────
        -- Formule simple : 0.0 (bounce) → 1.0 (checkout completé)
        -- Utilisé comme feature dans les modèles de propension à l'achat
        CASE
            WHEN sb.has_conversion                   THEN 1.0
            WHEN sb.add_to_cart_count > 0            THEN 0.7
            WHEN sb.product_views > 2                THEN 0.4
            WHEN sb.page_views > 1                   THEN 0.2
            ELSE 0.0
        END                                                     AS engagement_score,

        -- ── Audit pipeline ───────────────────────────────────────────────────
        sb.last_event_ingested_at                               AS ingested_at,
        CURRENT_TIMESTAMP()                                     AS silver_loaded_at

    FROM session_boundaries sb
    LEFT JOIN session_pages sp ON sb.session_id = sp.session_id

    -- Filtre qualité final : sessions valides uniquement
    WHERE sb.total_events          >= 2       -- Au moins 2 events réels
      AND sb.session_duration_seconds >= 2    -- Durée minimale 2 secondes
      AND sb.session_id IS NOT NULL

)

-- Point d'entrée dbt
SELECT * FROM final