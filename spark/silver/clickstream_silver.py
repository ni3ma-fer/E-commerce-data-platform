# spark/silver/clickstream_silver.py
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_timestamp, to_date, trim, lower, when, lit,
    regexp_extract, count, datediff, current_date, row_number
)
from pyspark.sql.window import Window
import logging

# CORRECTION 3 — Remplacement de print() par le module logging standard.
# En production Airflow, les print() ne sont pas capturés par le système de logs
# des workers Spark. logging permet de configurer des niveaux (INFO/WARNING/ERROR),
# de router vers des handlers (fichier, CloudWatch, Azure Monitor) et d'horodater
# automatiquement chaque message — indispensable pour le debugging post-mortem.
logger = logging.getLogger('clickstream-silver')

spark = SparkSession.builder.appName('KiVendTout-Clickstream-Silver') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .getOrCreate()

BASE = 'abfss://medallion@kivendtoutstorage.dfs.core.windows.net'

def run_clickstream_silver(run_date: str):
    # Lecture Bronze incrémentale
    year, month, day = run_date.split('-')
    bronze = (
        spark.read.format('delta').load(f'{BASE}/bronze/clickstream')
        .filter(
            (col('year')  == int(year))  &
            (col('month') == int(month)) &
            (col('day')   == int(day))
        )
    )

    silver = (
        bronze
        # ── Typage ──────────────────────────────────────────────
        .withColumn('event_timestamp', to_timestamp(col('timestamp'), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn('event_date',        to_date(col('event_timestamp')))
        .withColumn('duration_seconds',  col('duration_seconds').cast('integer'))
        .withColumn('scroll_depth_pct',  col('scroll_depth_pct').cast('integer'))

        # ── Nettoyage ────────────────────────────────────────────
        # Normaliser le device_type en catégories propres
        .withColumn('device_category',
            when(col('device_type').startswith('mobile'), 'mobile')
            .when(col('device_type').startswith('tablet'), 'tablet')
            .otherwise('desktop'))

        # Extraire le navigateur depuis device_type
        .withColumn('browser',
            when(col('device_type').contains('chrome'),  'chrome')
            .when(col('device_type').contains('firefox'), 'firefox')
            .when(col('device_type').contains('safari'),  'safari')
            .otherwise('other'))

        # Catégoriser le referrer
        .withColumn('traffic_source',
            when(col('referrer') == 'google.com',      'organic_search')
            .when(col('referrer') == 'facebook.com',   'social')
            .when(col('referrer') == 'email_campaign', 'email')
            .when(col('referrer') == 'direct',         'direct')
            .otherwise('other'))

        # CORRECTION 3 (suite) — Filtre qualité avant déduplication.
        # Un event_id NULL ne peut pas servir de clé de déduplication :
        # row_number().over(Window.partitionBy(NULL)) regroupe tous les NULLs
        # dans une même partition géante, ne conserve qu'une seule ligne et
        # élimine silencieusement des événements potentiellement valides.
        # Ce filtre amont garantit que la fenêtre de déduplication opère
        # uniquement sur des clés exploitables.
        .filter(col('event_id').isNotNull())

        # ── Déduplication sur event_id ───────────────────────────
        .withColumn('rn', row_number().over(
            Window.partitionBy('event_id').orderBy(col('ingested_at').desc())))
        .filter(col('rn') == 1)
        .drop('rn', 'kafka_offset', 'kafka_partition', 'kafka_topic',
              'year', 'month', 'day', 'timestamp')
    )

    # Suppression des sessions trop courtes (bots probables)
    silver = silver.filter(col('duration_seconds') >= 2)

    silver.cache()
    row_count = silver.count()  # Première et unique évaluation complète du DAG

  
    (
        silver.write
        .format('delta')
        .mode('overwrite')
        .option('replaceWhere', f"event_date = '{run_date}'")  # Overwrite ciblé
        .option('mergeSchema', 'true')
        .partitionBy('event_date')
        .save(f'{BASE}/silver/clickstream')
    )

  
    silver.unpersist()

    # CORRECTION 3 (suite) — logger.info() avec la variable row_count pré-calculée.
    # row_count est déjà connu (compté avant le write), aucun recalcul ici.
    logger.info(f'Silver clickstream : {row_count} événements écrits pour {run_date}')

if __name__ == '__main__':
    from datetime import datetime
    run_clickstream_silver(datetime.today().strftime('%Y-%m-%d'))