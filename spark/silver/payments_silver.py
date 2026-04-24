# spark/silver/payments_silver.py
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_timestamp, to_date, trim, upper, lower, when, lit,
    sha2, concat_ws, datediff, current_date, lag, count, sum as spark_sum,
    window, avg, stddev, abs as spark_abs
)
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable
import logging

logger = logging.getLogger('payments-silver')

spark = SparkSession.builder \
    .appName('KiVendTout-Payments-Silver') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .getOrCreate()

BASE        = 'abfss://medallion@kivendtoutstorage.dfs.core.windows.net'
BRONZE_PATH = f'{BASE}/bronze/payments'
SILVER_PATH = f'{BASE}/silver/payments'

# ─── ÉTAPE 1 : Lecture Bronze ──────────────────────────────────
def read_bronze(run_date: str) -> DataFrame:
    """
    Lecture incrémentale : on ne retraite que la journée en cours.
    Optimisation critique : évite de relire tout l'historique Bronze à chaque run.
    """
    year, month, day = run_date.split('-')
    return (
        spark.read
        .format('delta')
        .load(BRONZE_PATH)
        # Filtre sur les colonnes de partition → scan partiel uniquement
        .filter((col('year') == int(year)) &
                (col('month') == int(month)) &
                (col('day') == int(day)))
    )

# ─── ÉTAPE 2 : Nettoyage et typage ────────────────────────────
def clean_and_type(df: DataFrame) -> DataFrame:
    """
    Normalise les types de données.
    Bronze : tout est StringType ou approx.
    Silver : chaque colonne a son type exact.
    """
    return (
        df
        # Timestamp ISO 8601 → TimestampType Spark
        .withColumn('event_timestamp', to_timestamp(col('timestamp'), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn('event_date', to_date(col('event_timestamp')))

        # Montant : arrondi à 2 décimales, valeur absolue (évite les négatifs Bronze)
        .withColumn('amount_eur', spark_abs(col('amount').cast(DoubleType())).cast(DecimalType(12, 2)))

        # Devise : standardisation en majuscules, valeur par défaut EUR
        .withColumn('currency_code', when(col('currency').isNull(), 'EUR')
                                      .otherwise(upper(trim(col('currency')))))

        # Pays : standardisation ISO 3166 (majuscules, 2 caractères)
        .withColumn('billing_country_iso',  upper(trim(col('billing_country'))))
        .withColumn('shipping_country_iso', upper(trim(col('shipping_country'))))

        # Flag transactions cross-border (pays de facturation ≠ livraison)
        .withColumn('is_cross_border',
            col('billing_country_iso') != col('shipping_country_iso'))

        # Supprimer les colonnes Bronze internes (offset, partition Kafka)
        .drop('kafka_offset', 'kafka_partition', 'kafka_topic',
              'year', 'month', 'day', 'timestamp')
    )

# ─── ÉTAPE 3 : Déduplication ───────────────────────────────────
def deduplicate(df: DataFrame) -> DataFrame:
    """
    Supprime les doublons basés sur transaction_id.
    Un paiement peut arriver plusieurs fois si le producteur retry.
    On garde la version la plus récente (ingested_at MAX).
    """
    from pyspark.sql.functions import row_number
    window_spec = Window.partitionBy('transaction_id').orderBy(col('ingested_at').desc())
    return (
        df
        .withColumn('row_num', row_number().over(window_spec))
        .filter(col('row_num') == 1)
        .drop('row_num')
    )

# ─── ÉTAPE 4 : SUPPRIMÉE — compute_behavioral_features ─────────
# OPTIMISATION 1 — CORRECTION LOGIQUE (paradoxe fenêtre 30 jours) :
# Cette fonction calculait des métriques glissantes sur 30 jours (avg montant,
# nb transactions, etc.) alors que le pipeline ne lit qu'UN SEUL jour de données
# en Bronze. Window.rangeBetween sur 1 jour = calcul sans historique → résultat
# statistiquement faux et trompeur pour XGBoost.
# Ce calcul est délégué à la couche Gold via dbt, qui dispose de la totalité
# de l'historique Silver pour construire des features correctes.

# ─── ÉTAPE 5 : Validation des données ──────────────────────────
def validate(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Sépare les enregistrements valides des invalides sans shuffle.

    OPTIMISATION 2 — ANTI-PATTERN subtract() :
    L'ancienne implémentation faisait df.subtract(valid_df) pour obtenir les
    rejets. subtract() est un Wide Transformation qui déclenche un Full Shuffle
    (hash partitioning des deux DataFrames + comparaison clé par clé).
    Sur un volume journalier e-commerce, cela représente plusieurs GB de données
    redistribuées inutilement sur le réseau.

    Solution : définir UNE SEULE condition de validité booléenne, puis appliquer
    filter(cond) et filter(~cond). Les deux opérations sont de simples scans
    locaux (Narrow Transformation) — zéro shuffle, zéro réseau.
    """
    valid_currencies = ['EUR', 'USD', 'GBP', 'CHF', 'JPY', 'CAD', 'AUD']

    # Condition de validité unique — réutilisée pour les deux chemins
    cond_valid = (
        col('transaction_id').isNotNull() &
        col('user_id').isNotNull() &
        (col('amount_eur') > 0) &
        col('currency_code').isin(valid_currencies) &
        col('event_timestamp').isNotNull()
    )

    # Narrow transformations — pas de shuffle, même DAG physique
    valid_df   = df.filter(cond_valid)
    invalid_df = df.filter(~cond_valid)  # Complément logique, O(n) local

    return valid_df, invalid_df

# ─── ÉCRITURE Silver avec MERGE (UPSERT) ────────────────────────
def write_silver(df: DataFrame, run_date: str):
    """
    Utilise MERGE INTO Delta pour faire un upsert.
    Si la transaction existe déjà en Silver : mise à jour.
    Si elle est nouvelle : insertion.
    Évite les doublons même si le job est relancé (idempotence).
    """
    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        silver_table = DeltaTable.forPath(spark, SILVER_PATH)
        (
            silver_table.alias('existing')
            .merge(
                df.alias('incoming'),
                'existing.transaction_id = incoming.transaction_id'
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info(f'MERGE Silver paiements terminé pour {run_date}')
    else:
        (
            df.write
            .format('delta')
            .mode('overwrite')
            .option('overwriteSchema', 'true')
            .partitionBy('event_date')
            .save(SILVER_PATH)
        )
        logger.info(f'Création Silver paiements : table initialisée')

# ─── PIPELINE COMPLET ──────────────────────────────────────────
def run_pipeline(run_date: str):
    logger.info(f'=== Démarrage Silver Paiements pour {run_date} ===')

    df = read_bronze(run_date)
    df = clean_and_type(df)
    df = deduplicate(df)

    # OPTIMISATION 3 — MULTIPLES count() / RÉÉVALUATION DU DAG :
    # Spark est lazy : chaque action (.count(), .write) re-déclenche l'intégralité
    # du DAG depuis la source (lecture Bronze → typage → déduplication).
    # Sans cache, appeler count() avant et après validate() = 3 scans Bronze complets.
    # df.cache() matérialise le DataFrame dédupliqué en mémoire (niveau MEMORY_AND_DISK
    # par défaut) après la première action. Les actions suivantes (count, write)
    # lisent depuis le cache — zéro recalcul, zéro I/O Bronze supplémentaire.
    df.cache()

    valid_df, invalid_df = validate(df)

    # Les deux count() suivants lisent depuis le cache — pas de re-scan Bronze
    valid_count   = valid_df.count()
    invalid_count = invalid_df.count()

    logger.info(f'Bronze lu (dédupliqué) : {valid_count + invalid_count} lignes')
    logger.info(f'Silver prêt            : {valid_count} lignes valides')

    if invalid_count > 0:
        logger.warning(f'{invalid_count} enregistrements invalides → quarantaine')
        # write() lit également depuis le cache
        invalid_df.write.format('delta').mode('append') \
            .save(f'{BASE}/quarantine/payments')

    write_silver(valid_df, run_date)

    # Libération explicite du cache après usage — bonne pratique en production
    # pour ne pas saturer la mémoire des workers entre deux jobs Airflow
    df.unpersist()

    logger.info(f'=== Silver Paiements terminé pour {run_date} ===')

if __name__ == '__main__':
    from datetime import datetime
    run_date = datetime.today().strftime('%Y-%m-%d')
    run_pipeline(run_date)