# ml/fraud_detection/predict.py
"""
Scoring fraude en temps réel : Kafka payments → XGBoost → Gold Azure ADLS.
 
ARCHITECTURE FINOPS :
    Spark local[*] + modèle XGBoost chargé depuis MLflow local
    → score calculé dans chaque micro-batch (trigger 10 secondes)
    → résultats écrits sur Azure ADLS : gold/fraud_scores/ (Delta)
    → latence end-to-end cible : <200ms
"""
import os, sys
import pandas as pd
import mlflow.xgboost
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit, when,
    pandas_udf, year, month, dayofmonth
)
from pyspark.sql.types import DoubleType, StructType, StructField, StringType
from dotenv import load_dotenv
load_dotenv()
 
ACCOUNT   = os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
KEY       = os.environ["AZURE_STORAGE_ACCOUNT_KEY"]
CONTAINER = os.getenv("AZURE_CONTAINER_NAME", "medallion-data")
ADLS_BASE = f"abfss://{CONTAINER}@{ACCOUNT}.dfs.core.windows.net"
KAFKA     = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MLFLOW_URI= os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
 
# ── Chargement du modèle Champion depuis MLflow Registry ─────────────────
# Le modèle est chargé UNE SEULE FOIS au démarrage du job (optimisation).
# Avantage : pas de rechargement à chaque micro-batch → latence minimale.
mlflow.set_tracking_uri(MLFLOW_URI)
MODEL_URI = "models:/fraud-xgboost-champion/Production"
print(f"Chargement modèle MLflow : {MODEL_URI}")
loaded_model = mlflow.xgboost.load_model(MODEL_URI)
print("Modèle XGBoost chargé — prêt pour le scoring temps réel")
 
# Feature columns (ordre identique à l'entraînement — CRITIQUE)
FEATURE_COLS = [
    "txn_count_5min", "txn_count_1h", "txn_count_30d",
    "avg_amount_30d", "amount_vs_avg_ratio", "amount_stddev_30d",
    "geo_distance_km", "is_cross_border", "user_is_minor",
    "is_first_transaction", "is_new_card", "is_new_merchant",
    "ip_geo_mismatch",
]
 
# ── SparkSession locale avec JARs Azure + Delta ──────────────────────────
PACKAGES = ",".join([
    "io.delta:delta-spark_2.12:3.1.0",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.apache.hadoop:hadoop-azure:3.3.4",
    "com.microsoft.azure:azure-storage:8.6.6",
])
spark = (
    SparkSession.builder
    .appName("KiVendTout-FraudScoring-Local")
    .master("local[*]")
    .config("spark.jars.packages", PACKAGES)
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config(f"fs.azure.account.key.{ACCOUNT}.dfs.core.windows.net", KEY)
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
 
# ── Pandas UDF : scoring XGBoost vectorisé ───────────────────────────────
# Pandas UDF = fonction Python exécutée par batch sur les partitions Spark.
# AVANTAGE vs Row UDF : traitement vectorisé → 10x plus rapide.
@pandas_udf(DoubleType())
def score_fraud_udf(*feature_series) -> pd.Series:
    df = pd.concat(feature_series, axis=1)
    df.columns = FEATURE_COLS[:len(feature_series)]
    # Imputation rapide : médiane pour les numériques, 0 pour les binaires
    df = df.fillna(df.median(numeric_only=True)).fillna(0)
    proba = loaded_model.predict_proba(df)[:, 1]
    return pd.Series(proba)
 
# ── Schéma Kafka pour les paiements ──────────────────────────────────────
schema = StructType([
    StructField("transaction_id",     StringType()),
    StructField("user_id",            StringType()),
    StructField("txn_count_5min",     DoubleType()),
    StructField("txn_count_30d",      DoubleType()),
    StructField("avg_amount_30d",     DoubleType()),
    StructField("amount_vs_avg_ratio",DoubleType()),
    StructField("geo_distance_km",    DoubleType()),
    StructField("is_cross_border",    StringType()),
    StructField("user_is_minor",      StringType()),
    StructField("is_first_transaction",StringType()),
    StructField("is_new_card",        StringType()),
    StructField("ip_geo_mismatch",    StringType()),
])
 
# ── Lecture du stream Kafka (payments-raw) ────────────────────────────────
stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribe", "payments-raw")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", "5000")
    .load()
    .withColumn("data", from_json(col("value").cast("string"), schema))
    .select("data.*")
)
 
# ── Application du score + règle de décision métier ──────────────────────
feat_available = [c for c in FEATURE_COLS if c in stream.columns]
scored = (
    stream
    .withColumn(
        "fraud_probability",
        score_fraud_udf(*[col(c) for c in feat_available])
    )
    .withColumn(
        "fraud_decision",
        when(col("fraud_probability") >= 0.80, "BLOCK")          # Fraude certaine
        .when(col("fraud_probability") >= 0.50, "3DS_REQUIRED")  # 3D Secure
        .otherwise("APPROVED")                                    # Légitime
    )
    .withColumn("scored_at",      current_timestamp())
    .withColumn("model_version",  lit("fraud-xgboost-champion/Production"))
    .withColumn("year",           year(col("scored_at")))
    .withColumn("month",          month(col("scored_at")))
    .withColumn("day",            dayofmonth(col("scored_at")))
)
 
# ── Écriture Gold ADLS — Delta Lake ──────────────────────────────────────
query = (
    scored.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation",
            f"{ADLS_BASE}/checkpoints/fraud_scores")
    .option("mergeSchema", "true")
    .trigger(processingTime="10 seconds")   # Priorité fraude : 10s
    .partitionBy("year", "month", "day")
    .start(f"{ADLS_BASE}/gold/fraud_scores")
)
 
print("Stream scoring fraude actif")
print(f"Écriture vers : {ADLS_BASE}/gold/fraud_scores")
print("Ctrl+C pour arrêt propre.")
query.awaitTermination()
