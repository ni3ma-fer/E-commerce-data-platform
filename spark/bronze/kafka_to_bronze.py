"""
kafka_to_bronze.py — KiVendTout | Étape 2 · Ingestion Bronze
=============================================================
Stratégie FinOps : Spark LOCAL → Azure ADLS Gen2 (pas de cluster Databricks).

ARCHITECTURE :
    ┌──────────────┐    ┌─────────────────────────┐    ┌──────────────────────────┐
    │  Kafka Local │───▶│  Spark local[*]          │───▶│  Azure ADLS Gen2         │
    │  localhost   │    │  Micro-batch 10s / 30s   │    │  abfss://medallion/bronze │
    └──────────────┘    └─────────────────────────┘    └──────────────────────────┘

VARIABLES D'ENVIRONNEMENT REQUISES (.env) :
══════════════════════════════════════════════════════════════════════════════
  # Nom du compte de stockage (sans .dfs.core.windows.net)
  AZURE_STORAGE_ACCOUNT_NAME=kivendtoutstorage

  # Clé d'accès : portail Azure → Storage Account → "Access keys" → key1 → Show
  AZURE_STORAGE_ACCOUNT_KEY=xxxxxxx...xxx==

  # Conteneur cible dans lequel le Medallion est organisé
  AZURE_CONTAINER_NAME=medallion

  # Kafka local (Docker ou natif)
  KAFKA_BOOTSTRAP_SERVERS=localhost:9092

  # Niveau de logs Spark (WARN recommandé en dev, ERROR en prod)
  SPARK_LOG_LEVEL=WARN
══════════════════════════════════════════════════════════════════════════════

JARS TÉLÉCHARGÉS AUTOMATIQUEMENT AU 1ER LANCEMENT (~150 MB via Maven Central) :
    - io.delta:delta-spark_2.12:3.1.0
    - org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
    - org.apache.hadoop:hadoop-azure:3.3.4
    - com.microsoft.azure:azure-storage:8.6.6
  → Mis en cache dans ~/.ivy2, les runs suivants sont instantanés.

PRÉREQUIS SYSTÈME :
    Python  3.10 / 3.11
    Java    11 ou 17  (JAVA_HOME doit être défini)
    Spark   3.5.x  (pip install pyspark==3.5.1)

LANCEMENT :
    source .venv/bin/activate
    python spark/bronze/kafka_to_bronze.py

    # Ou avec plus de mémoire driver :
    spark-submit --driver-memory 4g spark/bronze/kafka_to_bronze.py
"""

import os
import sys
import signal
import logging
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit,
    year, month, dayofmonth, when,
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType,
    IntegerType, LongType,
)

# ─── Chargement .env ──────────────────────────────────────────────────────────
# Cherche docker/.env puis .env à la racine du projet
ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / "docker" / ".env")
load_dotenv(ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("kafka-to-bronze")


# =============================================================================
#  LECTURE DES VARIABLES D'ENVIRONNEMENT + VALIDATION
# =============================================================================
STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "")
STORAGE_KEY      = os.getenv("AZURE_STORAGE_ACCOUNT_KEY",  "")
CONTAINER        = os.getenv("AZURE_CONTAINER_NAME",       "medallion")
KAFKA_SERVERS    = os.getenv("KAFKA_BOOTSTRAP_SERVERS",    "localhost:9092")
LOG_LEVEL        = os.getenv("SPARK_LOG_LEVEL",            "WARN")

# Fail fast : inutile de démarrer Spark si Azure n'est pas configuré
missing = []
if not STORAGE_ACCOUNT: missing.append("AZURE_STORAGE_ACCOUNT_NAME")
if not STORAGE_KEY:      missing.append("AZURE_STORAGE_ACCOUNT_KEY")
if missing:
    log.critical(
        f"Variables d'environnement manquantes : {', '.join(missing)}\n"
        f"Vérifiez votre fichier .env (voir en-tête de ce script)."
    )
    sys.exit(1)

# Chemins ADLS (protocole abfss = Azure Blob File System Secure)
# Format : abfss://<container>@<account>.dfs.core.windows.net/<path>
ADLS_BASE       = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
BRONZE_BASE     = f"{ADLS_BASE}/bronze"
CHECKPOINT_BASE = f"{ADLS_BASE}/checkpoints/bronze"

log.info(f"Cible Azure  : {ADLS_BASE}")
log.info(f"Kafka        : {KAFKA_SERVERS}")


# =============================================================================
#  SPARK SESSION — LOCAL + JARS AZURE + DELTA
# =============================================================================
#
# POURQUOI CES JARS :
#   delta-spark             → Format Delta Lake (ACID, time travel, MERGE INTO)
#   spark-sql-kafka         → Connecteur Spark ↔ Kafka (lecture streaming native)
#   hadoop-azure:3.3.4      → Driver Hadoop pour le protocole abfss:// vers ADLS Gen2
#   azure-storage:8.6.6     → SDK bas niveau requis par hadoop-azure
#
# RÈGLE DE VERSION : hadoop-azure DOIT correspondre à la version Hadoop
# embarquée dans PySpark. PySpark 3.5.x embarque Hadoop 3.3.4.
#
SPARK_PACKAGES = ",".join([
    "io.delta:delta-spark_2.12:3.1.0",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.apache.hadoop:hadoop-azure:3.3.4",
    "com.microsoft.azure:azure-storage:8.6.6",
])

spark = (
    SparkSession.builder
    .appName("KiVendTout-Kafka-to-Bronze-Local")

    # ── Exécution locale : utilise tous les cœurs du CPU ───────────────────
    .master("local[*]")

    # ── JARs chargés dynamiquement (Maven Central) ─────────────────────────
    .config("spark.jars.packages", SPARK_PACKAGES)

    # ── Extensions Delta Lake ───────────────────────────────────────────────
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # ── Authentification Azure ADLS Gen2 via clé de compte ─────────────────
    # La propriété Hadoop fs.azure.account.key.<account>.dfs.core.windows.net
    # est le moyen le plus simple pour un dev local. En production, préférer
    # un Service Principal (OAuth2) ou Azure Managed Identity.
    .config(
        f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
        STORAGE_KEY,
    )

    # ── Optimisations mémoire pour usage local ─────────────────────────────
    .config("spark.driver.memory",             "4g")
    .config("spark.sql.shuffle.partitions",    "4")   # 200 par défaut = inutile en local
    .config("spark.sql.streaming.schemaInference", "false")

    .getOrCreate()
)

spark.sparkContext.setLogLevel(LOG_LEVEL)
log.info(f"SparkSession démarrée — Spark {spark.version}")


# =============================================================================
#  SCHÉMAS JSON EXACTS DES TOPICS KAFKA (Alignés avec les producteurs)
# =============================================================================

CLICKSTREAM_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("page", StringType(), True),
    StructField("page_path", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("device_fingerprint", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("scroll_depth_pct", IntegerType(), True),
    StructField("duration_seconds", IntegerType(), True),
    StructField("viewport_width", IntegerType(), True),
    StructField("is_bot_suspected", BooleanType(), True),
    StructField("utm_source", StringType(), True),
    StructField("utm_medium", StringType(), True),
    # Champs spécifiques au panier
    StructField("product_name", StringType(), True),
    StructField("product_price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("is_adult_product", BooleanType(), True),
    # Champs spécifiques KYC & Fraude
    StructField("document_type", StringType(), True),
    StructField("upload_method", StringType(), True),
    StructField("kyc_current_status", StringType(), True),
    StructField("failure_reason", StringType(), True),
    StructField("user_age", IntegerType(), True),
    StructField("document_path", StringType(), True),
    StructField("blocked_category", StringType(), True),
    StructField("kyc_status", StringType(), True),
    StructField("block_reason", StringType(), True),
    StructField("attempted_category", StringType(), True),
])

PAYMENT_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_category_id", StringType(), True),
    StructField("is_adult_product", BooleanType(), True),
    StructField("card_issuer", StringType(), True),
    StructField("billing_country", StringType(), True),
    StructField("shipping_country", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("ip_country_inferred", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("device_fingerprint", StringType(), True),
    StructField("is_new_device", BooleanType(), True),
    StructField("is_new_merchant", BooleanType(), True),
    StructField("user_kyc_status", StringType(), True),
    StructField("user_age", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("merchant_id", StringType(), True),
    # Champs liés à la fraude (Ground Truth pour ML)
    StructField("fraud_label_ground_truth", BooleanType(), True),
    StructField("fraud_scenario_label", StringType(), True),
    StructField("velocity_attack_group", StringType(), True),
    StructField("geo_mismatch_detail", StringType(), True),
    StructField("card_testing_batch_id", StringType(), True),
    StructField("account_takeover_indicator", StringType(), True),
])

LOGISTICS_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_category_id", StringType(), True),
    StructField("is_adult_product", BooleanType(), True),
    StructField("status", StringType(), True),
    StructField("previous_status", StringType(), True),
    StructField("carrier", StringType(), True),
    StructField("tracking_number", StringType(), True),
    StructField("warehouse_id", StringType(), True),
    StructField("warehouse_city", StringType(), True),
    StructField("delivery_city", StringType(), True),
    StructField("delivery_postal_code", StringType(), True),
    StructField("delivery_country", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("estimated_delivery", StringType(), True),
    StructField("actual_delivery", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("weight_kg", DoubleType(), True),
    StructField("nb_items", IntegerType(), True),
    StructField("is_delayed", BooleanType(), True),
    StructField("delay_reason", StringType(), True),
    StructField("shipping_cost_eur", DoubleType(), True),
    StructField("billing_shipping_country_match", BooleanType(), True),
    StructField("warehouse_stock_level", StringType(), True),
    StructField("customer_rating", IntegerType(), True),
])

# =============================================================================
#  LECTURE KAFKA → STREAMING DATAFRAME
# =============================================================================
def read_kafka_stream(topic: str) -> DataFrame:
    """
    Ouvre un flux Structured Streaming depuis un topic Kafka local.

    OPTIONS CLÉS :
        startingOffsets="latest"       → En DEV : n'ingère que les nouveaux messages.
                                          Mettre "earliest" pour rejouer l'historique.
        maxOffsetsPerTrigger=10_000    → Évite les OOM sur machine locale en limitant
                                          la taille de chaque micro-batch.
        failOnDataLoss=false           → Tolère les gaps d'offsets (ex: topic recréé).
                                          Mettre à true en production.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",       KAFKA_SERVERS)
        .option("subscribe",                     topic)
        .option("startingOffsets",               "latest")
        .option("maxOffsetsPerTrigger",          "10000")
        .option("failOnDataLoss",                "false")
        .option("kafka.session.timeout.ms",      "30000")
        .option("kafka.request.timeout.ms",      "40000")
        .option("kafka.heartbeat.interval.ms",   "10000")
        .load()
    )


# =============================================================================
#  TRANSFORMATION MINIMALE → BRONZE
# =============================================================================
def parse_and_tag(kafka_df: DataFrame, schema: StructType, source: str) -> DataFrame:
    """
    Transformations Bronze : désérialisation JSON + métadonnées d'ingestion.

    PRINCIPE COUCHE BRONZE :
        • On ne filtre RIEN — même les messages malformés sont stockés.
        • Un message dont le JSON est invalide génère data=NULL avec _parse_error=True.
        • Le champ _raw_json conserve la valeur brute pour investigation.
        • Tout le nettoyage et la validation sont délégués à Silver.

    COLONNES AJOUTÉES (audit trail) :
        ingested_at      → Timestamp exact d'ingestion (pour la fraîcheur des données)
        _source_topic    → Topic Kafka d'origine (utile en cas de fusion de topics)
        _ingestion_mode  → "local-spark" ici (vs "databricks" en prod)
        _parse_error     → Flag si le parsing JSON a échoué
        _raw_json        → Valeur brute conservée pour debug
        year/month/day   → Colonnes de partition pour les scans Silver optimisés
    """
    return (
        kafka_df
        # Décodage bytes → string
        .withColumn("raw_value", col("value").cast("string"))
        .withColumn("kafka_key", col("key").cast("string"))

        # Parsing JSON avec schéma strict
        # Si JSON invalide → from_json retourne NULL (tolérance Bronze)
        .withColumn("_parsed", from_json(col("raw_value"), schema))

        # Expansion des champs JSON au niveau supérieur
        .select(
            col("_parsed.*"),
            col("kafka_key"),
            col("offset")           .alias("kafka_offset"),
            col("partition")        .alias("kafka_partition"),
            col("topic")            .alias("kafka_topic"),
            col("raw_value")        .alias("_raw_json"),
            when(col("_parsed").isNull(), True)
            .otherwise(False)       .alias("_parse_error"),
            current_timestamp()     .alias("ingested_at"),
            lit(source)             .alias("_source_topic"),
            lit("local-spark")      .alias("_ingestion_mode"),
        )

        # Colonnes de partition temporelle
        .withColumn("year",  year (current_timestamp()))
        .withColumn("month", month(current_timestamp()))
        .withColumn("day",   dayofmonth(current_timestamp()))
    )


# =============================================================================
#  ÉCRITURE DELTA SUR AZURE ADLS GEN2
# =============================================================================
def write_bronze_delta(
    df:              DataFrame,
    delta_path:      str,
    checkpoint_path: str,
    trigger_secs:    int = 30,
) -> "StreamingQuery":
    """
    Écrit le stream au format Delta Lake sur Azure ADLS Gen2.

    CHOIX TECHNIQUES :
        outputMode="append"    → Bronze est append-only (données jamais modifiées).
        mergeSchema="true"     → Accepte l'évolution du schéma sans interrompre
                                 le stream (nouveau champ dans le JSON = OK).
        partitionBy            → Découpe le stockage par date pour des lectures
                                 efficaces en Silver (scan partiel).

    CHECKPOINTING ADLS :
        Le checkpoint est stocké sur Azure (pas en local) afin de survivre
        à un redémarrage de la machine. En cas de crash → reprise exacte
        au dernier offset Kafka confirmé (sémantique exactly-once).
    """
    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema",        "true")
        .trigger(processingTime=f"{trigger_secs} seconds")
        .partitionBy("year", "month", "day")
        .start(delta_path)
    )


# =============================================================================
#  POINT D'ENTRÉE PRINCIPAL
# =============================================================================
def main():
    log.info("=" * 65)
    log.info("KiVendTout  —  Spark LOCAL  →  Bronze Azure ADLS Gen2")
    log.info("=" * 65)

    queries = []

    # ── Stream 1 : Clickstream (trigger 30s) ─────────────────────────────────
    log.info("[1/3] Démarrage stream clickstream-raw → Bronze/clickstream")
    q1 = write_bronze_delta(
        parse_and_tag(read_kafka_stream("clickstream-raw"), CLICKSTREAM_SCHEMA, "clickstream"),
        delta_path      = f"{BRONZE_BASE}/clickstream",
        checkpoint_path = f"{CHECKPOINT_BASE}/clickstream",
        trigger_secs    = 30,
    )
    queries.append(("clickstream", q1))
    log.info(f"  ✓ Stream clickstream actif — ID: {q1.id}")

    # ── Stream 2 : Paiements (trigger 10s — priorité fraude temps réel) ──────
    log.info("[2/3] Démarrage stream payments-raw → Bronze/payments")
    q2 = write_bronze_delta(
        parse_and_tag(read_kafka_stream("payments-raw"), PAYMENT_SCHEMA, "payments"),
        delta_path      = f"{BRONZE_BASE}/payments",
        checkpoint_path = f"{CHECKPOINT_BASE}/payments",
        trigger_secs    = 10,   # Réduit pour la détection de fraude quasi-temps-réel
    )
    queries.append(("payments", q2))
    log.info(f"  ✓ Stream payments actif — ID: {q2.id}")

    # ── Stream 3 : Logistique (trigger 60s) ──────────────────────────────────
    log.info("[3/3] Démarrage stream logistics-raw → Bronze/logistics")
    q3 = write_bronze_delta(
        parse_and_tag(read_kafka_stream("logistics-raw"), LOGISTICS_SCHEMA, "logistics"),
        delta_path      = f"{BRONZE_BASE}/logistics",
        checkpoint_path = f"{CHECKPOINT_BASE}/logistics",
        trigger_secs    = 60,
    )
    queries.append(("logistics", q3))
    log.info(f"  ✓ Stream logistics actif — ID: {q3.id}")

    log.info("─" * 65)
    log.info(f"3 streams actifs — écriture vers : {BRONZE_BASE}")
    log.info(f"Checkpoints sur Azure : {CHECKPOINT_BASE}")
    log.info("Ctrl+C pour arrêt propre.")
    log.info("─" * 65)

    # ── Arrêt propre sur signal ───────────────────────────────────────────────
    def _stop(sig, frame):
        log.info("Signal reçu — arrêt propre...")
        for name, q in queries:
            try:
                q.stop()
                log.info(f"  → Stream '{name}' arrêté.")
            except Exception as e:
                log.warning(f"  → Erreur arrêt '{name}': {e}")
        spark.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT,  _stop)
    signal.signal(signal.SIGTERM, _stop)

    # ── Boucle de monitoring ──────────────────────────────────────────────────
    try:
        while True:
            for name, q in queries:
                if not q.isActive:
                    log.error(f"ALERTE : stream '{name}' mort — {q.exception()}")
                elif q.lastProgress:
                    p = q.lastProgress
                    log.info(
                        f"[{name}] batch#{p.get('batchId','?')} — "
                        f"{p.get('numInputRows', 0)} msgs → "
                        f"{p.get('numOutputRows', 0)} lignes Delta"
                    )
            spark.streams.awaitAnyTermination(timeout=30)
    except KeyboardInterrupt:
        _stop(None, None)


if __name__ == "__main__":
    main()


    