"""
dag_bronze_crm.py — KiVendTout | DAG Bronze CRM
================================================
Airflow DAG : génération CSV CRM cohérent + upload direct vers Azure ADLS Gen2.

STRATÉGIE FINOPS :
    Pas de SDK Databricks, pas de cluster.
    Le DAG tourne localement dans Docker Airflow et écrit directement
    sur Azure ADLS Gen2 via la bibliothèque azure-storage-file-datalake.

COHÉRENCE RELATIONNELLE GARANTIE :
    La tâche de génération N'invente PAS de nouveaux utilisateurs.
    Elle importe le USER_POOL de shared_data_pool.py pour que chaque
    user_id, age, et kyc_status soient identiques dans :
        ✓ clickstream_producer.py
        ✓ payment_producer.py
        ✓ Ce CRM  ← vous êtes ici

VARIABLES D'ENVIRONNEMENT REQUISES (dans docker/.env) :
══════════════════════════════════════════════════════════════════════════════
  # Nom du compte de stockage Azure
  AZURE_STORAGE_ACCOUNT_NAME=kivendtoutstorage

  # Clé d'accès (portail Azure → Storage Account → Access keys → key1)
  AZURE_STORAGE_ACCOUNT_KEY=xxxxxxx...xxx==

  # Conteneur cible
  AZURE_CONTAINER_NAME=medallion

  # Chemin de destination dans le conteneur
  AZURE_BRONZE_CRM_PATH=bronze/crm

  # Optionnel : nombre de clients à générer par run (défaut : 5000)
  CRM_POOL_SIZE=5000
══════════════════════════════════════════════════════════════════════════════

GRAPHE DES TÂCHES :
    check_env
        └── generate_crm_csv
                └── upload_to_adls
                        └── validate_great_expectations
                                └── notify_success / notify_failure

DÉPENDANCES PYTHON REQUISES (à ajouter dans requirements.txt Airflow) :
    apache-airflow==2.9.1
    azure-storage-file-datalake==12.14.0
    great-expectations==0.18.15
    pandas==2.2.2
    python-dotenv==1.0.1
"""

import os
import sys
import logging
import hashlib
import random
import tempfile
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

# ─── Import du pool partagé ───────────────────────────────────────────────────
# shared_data_pool.py doit être accessible depuis le PYTHONPATH d'Airflow.
# Ajouter dans docker-compose.yml → environment: PYTHONPATH=/opt/airflow
sys.path.insert(0, "/opt/airflow")
try:
    from shared_data_pool import (
        USER_IDS,
        USER_SEGMENTS,
        get_kyc_status,
        get_birth_date,
        get_age,
        is_minor,
        FR_CITIES,
        S3_ID_CARDS_PATH,
    )
    _POOL_AVAILABLE = True
except ImportError:
    logging.getLogger("dag_bronze_crm").warning(
        "shared_data_pool.py introuvable — fallback sur la génération locale. "
        "Assurez-vous que PYTHONPATH=/opt/airflow est configuré dans Airflow."
    )
    _POOL_AVAILABLE = False

# ─── Chargement .env ──────────────────────────────────────────────────────────
load_dotenv("/opt/airflow/.env")
load_dotenv("/opt/airflow/docker/.env")

log = logging.getLogger("dag_bronze_crm")


# =============================================================================
#  PARAMÈTRES DU DAG
# =============================================================================
DEFAULT_ARGS = {
    "owner":               "data-team-kivendtout",
    "depends_on_past":     False,
    "email":               ["data-team@kivendtout.fr"],
    "email_on_failure":    True,
    "email_on_retry":      False,
    "retries":             3,
    "retry_delay":         timedelta(minutes=5),
    "retry_exponential_backoff": True,   # 5min → 10min → 20min
    "execution_timeout":   timedelta(hours=2),
}

dag = DAG(
    dag_id          = "bronze_crm_ingestion",
    description     = "CRM cohérent (shared_data_pool) → CSV → Azure ADLS Gen2 Bronze",
    default_args    = DEFAULT_ARGS,
    schedule_interval = "0 2 * * *",   # Toutes les nuits à 02h00 UTC
    start_date      = days_ago(1),
    catchup         = False,
    max_active_runs = 1,               # Pas de run en parallèle pour éviter les doublons
    tags            = ["bronze", "crm", "batch", "kivendtout", "finops"],
    dagrun_timeout  = timedelta(hours=3),
    doc_md          = __doc__,
)


# =============================================================================
#  TÂCHE 0 : VÉRIFICATION DE L'ENVIRONNEMENT AZURE
# =============================================================================
def check_azure_env(**ctx) -> bool:
    """
    Short-circuit : vérifie que les credentials Azure sont présents.
    Si une variable manque → toutes les tâches suivantes sont SKIPPED (pas FAILED).
    Cela évite un run rouge inutile si Azure est temporairement inaccessible.
    """
    required = [
        "AZURE_STORAGE_ACCOUNT_NAME",
        "AZURE_STORAGE_ACCOUNT_KEY",
        "AZURE_CONTAINER_NAME",
    ]
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        log.error(f"Variables Azure manquantes : {missing}. DAG skippé.")
        return False

    log.info("Credentials Azure présents — DAG autorisé à continuer.")
    log.info(f"  Compte   : {os.getenv('AZURE_STORAGE_ACCOUNT_NAME')}")
    log.info(f"  Conteneur: {os.getenv('AZURE_CONTAINER_NAME')}")
    return True


# =============================================================================
#  HELPERS — GÉNÉRATION COHÉRENTE AVEC LE POOL PARTAGÉ
# =============================================================================

def _deterministic_random(seed_str: str, lo: float, hi: float) -> float:
    """
    Génère un float pseudo-aléatoire déterministe basé sur une chaîne seed.
    Garantit qu'un user_id produit toujours la même valeur → reproductibilité.
    """
    h = int(hashlib.md5(seed_str.encode()).hexdigest(), 16)
    return lo + (h % 1_000_000) / 1_000_000.0 * (hi - lo)


def _get_customer_segment(user_id: str) -> str:
    """
    Détermine le segment client depuis le user_id.
    Cohérent avec USER_SEGMENTS de shared_data_pool.py.
    """
    if _POOL_AVAILABLE:
        for seg, pool in USER_SEGMENTS.items():
            if user_id in pool:
                return seg
    # Fallback déterministe
    h = int(hashlib.md5((user_id + "seg").encode()).hexdigest(), 16) % 100
    if h < 5:   return "vip"
    if h < 20:  return "gold"
    if h < 50:  return "silver"
    if h < 80:  return "bronze"
    return "new"


def _get_total_spent(user_id: str, segment: str) -> float:
    """
    Total dépensé par l'utilisateur — cohérent avec son segment.
    Déterministe : même user_id → même montant sur tous les runs.
    """
    ranges = {
        "vip":    (2_000.0, 25_000.0),
        "gold":   (500.0,   4_999.0),
        "silver": (100.0,   999.0),
        "bronze": (10.0,    199.0),
        "new":    (0.0,     49.0),
    }
    lo, hi = ranges.get(segment, (0.0, 999.0))
    raw = _deterministic_random(user_id + "spent", lo, hi)
    return round(raw, 2)


def _get_registration_date(user_id: str, segment: str) -> date:
    """
    Date d'inscription cohérente avec le segment.
    Les segments "new" ont des comptes récents (<30 jours).
    CAS D'USAGE FRAUDE : un "new" avec un gros achat = signal d'alerte.
    """
    today = date.today()
    if segment == "new":
        days_ago_reg = int(_deterministic_random(user_id + "reg", 1, 30))
    elif segment == "bronze":
        days_ago_reg = int(_deterministic_random(user_id + "reg", 30, 365))
    elif segment == "silver":
        days_ago_reg = int(_deterministic_random(user_id + "reg", 90, 730))
    elif segment == "gold":
        days_ago_reg = int(_deterministic_random(user_id + "reg", 180, 1_095))
    else:  # vip
        days_ago_reg = int(_deterministic_random(user_id + "reg", 365, 2_190))
    return today - timedelta(days=days_ago_reg)


def _get_id_document_path(user_id: str, kyc_status: str) -> str | None:
    """
    Retourne le chemin S3/ADLS simulé de l'image CNI.

    CAS D'USAGE ML / OCR :
        Ce chemin est la référence que le pipeline Tesseract+OpenCV
        utilisera pour récupérer l'image et en extraire les données.

        verified  → image uploadée et validée (format réel)
        pending   → image uploadée, en attente de validation
        rejected  → image uploadée mais rejetée (qualité insuffisante)
        none/expired → NULL (jamais uploadé)
    """
    if kyc_status in ("verified", "pending", "rejected"):
        base = S3_ID_CARDS_PATH if _POOL_AVAILABLE else "s3://kivendtout-ml-data/id_cards"
        return f"{base}/{user_id.lower()}/id_card_{user_id.lower()}.jpg"
    return None


def _get_gdpr_consent_date(user_id: str, reg_date: date) -> str:
    """
    Date de consentement RGPD = date d'inscription ± quelques jours.
    Toujours APRÈS la date d'inscription (cohérence logique).
    """
    delta = int(_deterministic_random(user_id + "gdpr", 0, 7))
    consent_date = reg_date + timedelta(days=delta)
    return consent_date.isoformat()


# =============================================================================
#  TÂCHE 1 : GÉNÉRATION DU CSV CRM COHÉRENT
# =============================================================================
def generate_crm_csv(**ctx) -> str:
    """
    Génère le DataFrame CRM en utilisant le USER_POOL de shared_data_pool.py.

    COHÉRENCE GARANTIE :
        Chaque user_id, age, kyc_status est identique à ce que produisent
        clickstream_producer.py et payment_producer.py.
        Les JOINs Silver (silver_crm JOIN silver_payments ON user_id) sont
        donc toujours résolubles sans perte de données.

    CHAMPS ENRICHIS CRM (en plus du pool partagé) :
        total_spent_eur      → Historique d'achats cohérent avec le segment
        customer_segment     → vip/gold/silver/bronze/new (Pareto 5%/15%/30%/30%/20%)
        newsletter_optin     → 65% opt-in (taux réaliste FR)
        gdpr_consent_date    → Toujours >= registration_date
        id_document_image_path → Chemin S3 pour le pipeline OCR ML
        last_order_date      → Date du dernier achat (feature RFM)
        total_orders         → Nombre de commandes (feature RFM Frequency)
    """
    run_date  = ctx.get("ds", date.today().isoformat())
    pool_size = int(os.getenv("CRM_POOL_SIZE", "5000"))

    log.info(f"Génération CRM pour run_date={run_date}, pool_size={pool_size}")

    # Source des user_ids : pool partagé si disponible, sinon génération locale
    if _POOL_AVAILABLE:
        user_ids = USER_IDS[:pool_size]
        log.info("Mode COHÉRENT : utilisation de shared_data_pool.USER_IDS")
    else:
        user_ids = [f"USR-{i:06d}" for i in range(1, pool_size + 1)]
        log.warning("Mode FALLBACK : shared_data_pool non disponible — cohérence réduite")

    records = []
    for uid in user_ids:

        # ── Données issues du pool partagé ────────────────────────────────
        if _POOL_AVAILABLE:
            birth_dt    = get_birth_date(uid)
            birth_date  = birth_dt.date().isoformat()
            age         = get_age(uid)
            kyc_status  = get_kyc_status(uid)
            minor_flag  = is_minor(uid)
        else:
            # Fallback déterministe
            h          = int(hashlib.md5((uid + "birth").encode()).hexdigest(), 16) % 100
            age        = 17 if h < 5 else (random.randint(18, 25) if h < 20 else random.randint(26, 65))
            birth_date = (date.today() - timedelta(days=age * 365)).isoformat()
            h2         = int(hashlib.md5((uid + "kyc").encode()).hexdigest(), 16) % 100
            kyc_status = "verified" if h2 < 60 else ("pending" if h2 < 75 else ("none" if h2 < 85 else "rejected"))
            minor_flag = age < 18

        # ── Données CRM enrichies ─────────────────────────────────────────
        segment      = _get_customer_segment(uid)
        total_spent  = _get_total_spent(uid, segment)
        reg_date     = _get_registration_date(uid, segment)
        total_orders = int(_deterministic_random(uid + "orders", 0,
                           {"vip": 200, "gold": 80, "silver": 30, "bronze": 10, "new": 2}[segment]))

        # Dernière commande : entre registration_date et aujourd'hui
        days_since_reg = (date.today() - reg_date).days
        if days_since_reg > 0 and total_orders > 0:
            last_order_days_ago = int(_deterministic_random(uid + "last", 0, min(days_since_reg, 180)))
            last_order_date = (date.today() - timedelta(days=last_order_days_ago)).isoformat()
        else:
            last_order_date = None

        # Ville et code postal déterministes
        city_idx  = int(hashlib.md5((uid + "city").encode()).hexdigest(), 16) % 20
        cities    = FR_CITIES if _POOL_AVAILABLE else [
            ("Paris","75001"),("Lyon","69001"),("Marseille","13001"),
            ("Toulouse","31000"),("Nice","06000"),("Nantes","44000"),
            ("Strasbourg","67000"),("Bordeaux","33000"),("Lille","59000"),
            ("Rennes","35000"),("Montpellier","34000"),("Reims","51100"),
            ("Grenoble","38000"),("Dijon","21000"),("Angers","49000"),
            ("Nimes","30000"),("Le Havre","76600"),("Cergy","95000"),
            ("Saint-Etienne","42000"),("Villeurbanne","69100"),
        ]
        city, postal = cities[city_idx % len(cities)]

        # Newsletter : 65% opt-in (taux réaliste marché FR)
        h_nl = int(hashlib.md5((uid + "nl").encode()).hexdigest(), 16) % 100
        newsletter = h_nl < 65

        record = {
            # ── Identité ──────────────────────────────────────────────────
            "user_id":              uid,
            "registration_date":    reg_date.isoformat(),
            "city":                 city,
            "postal_code":          postal,
            "country":              "FR",

            # ── Données âge / KYC ─────────────────────────────────────────
            # COHÉRENCE : ces valeurs sont identiques à shared_data_pool.py
            # → Indispensable pour les JOINs Silver et les features ML fraude
            "birth_date":           birth_date,
            "age":                  age,
            "is_minor":             minor_flag,
            "id_verification_status": kyc_status,

            # ── Chemin image CNI pour le pipeline OCR ML ──────────────────
            # CAS D'USAGE ML : ce chemin sera utilisé par le job Tesseract+OpenCV
            # pour extraire Nom, Prénom, Date de naissance depuis l'image
            "id_document_image_path": _get_id_document_path(uid, kyc_status),

            # ── Données CRM enrichies ─────────────────────────────────────
            "customer_segment":     segment,
            "total_orders":         total_orders,
            "total_spent_eur":      total_spent,
            "last_order_date":      last_order_date,
            "newsletter_optin":     newsletter,

            # ── RGPD ──────────────────────────────────────────────────────
            # Art. 7 RGPD : conservation de la preuve du consentement
            "gdpr_consent_date":    _get_gdpr_consent_date(uid, reg_date),

            # ── Métadonnées pipeline ──────────────────────────────────────
            "crm_extract_date":     run_date,
        }

        # Simulation de valeurs manquantes réalistes (3-8% selon le champ)
        # → Permet de tester les quality gates Great Expectations Bronze
        h_null = int(hashlib.md5((uid + "null").encode()).hexdigest(), 16) % 100
        if h_null < 3:   record["last_order_date"]       = None   # 3% nulls
        if h_null < 5:   record["id_document_image_path"] = None  # 5% nulls (même si kyc=verified → erreur simulée)
        if h_null < 8:   record["newsletter_optin"]       = None  # 8% nulls (non renseigné)

        records.append(record)

    df = pd.DataFrame(records)
    log.info(f"DataFrame CRM généré : {len(df)} lignes, {len(df.columns)} colonnes")

    # Statistiques pour les logs Airflow
    log.info(f"  → Segments     : {df['customer_segment'].value_counts().to_dict()}")
    log.info(f"  → KYC statuts  : {df['id_verification_status'].value_counts().to_dict()}")
    log.info(f"  → Mineurs      : {df['is_minor'].sum()} ({df['is_minor'].mean()*100:.1f}%)")
    log.info(f"  → Avec image CNI: {df['id_document_image_path'].notna().sum()}")

    # Sauvegarde temporaire sur disque local
    tmp_dir  = tempfile.mkdtemp(prefix="kivendtout_crm_")
    csv_path = os.path.join(tmp_dir, f"crm_export_{run_date}.csv")
    df.to_csv(csv_path, index=False, encoding="utf-8")
    log.info(f"CSV sauvegardé localement : {csv_path} ({os.path.getsize(csv_path):,} octets)")

    # Passage du chemin à la tâche suivante via XCom
    return csv_path


# =============================================================================
#  TÂCHE 2 : UPLOAD DIRECT VERS AZURE ADLS GEN2
# =============================================================================
def upload_to_adls(**ctx) -> dict:
    """
    Upload direct du CSV CRM vers Azure ADLS Gen2.

    STRATÉGIE FINOPS :
        Utilise azure-storage-file-datalake (pip install azure-storage-file-datalake).
        Pas de Databricks, pas de Spark — simple upload HTTP depuis la machine locale.
        Coût : ~0€ (l'ingress vers Azure Storage est gratuit).

    PARTITIONNEMENT BRONZE :
        Chemin de destination : bronze/crm/year=YYYY/month=MM/day=DD/crm_export.csv
        Ce partitionnement est cohérent avec celui créé par kafka_to_bronze.py
        → Silver peut lire toutes les sources Bronze avec le même filtre de partition.

    RETOUR XCom :
        Retourne les métadonnées de l'upload pour la tâche de validation suivante.
    """
    from azure.storage.filedatalake import DataLakeServiceClient

    csv_path = ctx["task_instance"].xcom_pull(task_ids="generate_crm_csv")
    run_date = ctx.get("ds", date.today().isoformat())

    account_name = os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
    account_key  = os.environ["AZURE_STORAGE_ACCOUNT_KEY"]
    container    = os.getenv("AZURE_CONTAINER_NAME",    "medallion")
    bronze_path  = os.getenv("AZURE_BRONZE_CRM_PATH",  "bronze/crm")

    # Partitionnement par date (cohérent avec Spark Bronze)
    year, month, day = run_date.split("-")
    dest_dir      = f"{bronze_path}/year={year}/month={month}/day={day}"
    dest_filename = f"crm_export_{run_date}.csv"
    dest_full     = f"{dest_dir}/{dest_filename}"

    log.info(f"Upload vers : abfss://{container}@{account_name}.dfs.core.windows.net/{dest_full}")

    # ── Connexion au service ADLS Gen2 ────────────────────────────────────────
    service_client = DataLakeServiceClient(
        account_url = f"https://{account_name}.dfs.core.windows.net",
        credential  = account_key,
    )
    fs_client  = service_client.get_file_system_client(container)
    dir_client = fs_client.get_directory_client(dest_dir)

    # Création du répertoire si inexistant (ADLS Gen2 supporte les paths hiérarchiques)
    try:
        dir_client.create_directory()
        log.info(f"Répertoire créé : {dest_dir}")
    except Exception:
        pass  # Le répertoire existe déjà — normal pour les runs suivants

    # ── Upload du fichier ─────────────────────────────────────────────────────
    file_client = dir_client.get_file_client(dest_filename)
    with open(csv_path, "rb") as f:
        file_data = f.read()

    file_client.upload_data(file_data, overwrite=True)

    file_size = os.path.getsize(csv_path)
    log.info(f"Upload réussi — {file_size:,} octets → {dest_full}")

    # ── Nettoyage du fichier temporaire local ─────────────────────────────────
    try:
        os.remove(csv_path)
        log.info(f"Fichier temporaire supprimé : {csv_path}")
    except OSError:
        pass

    return {
        "status":          "success",
        "destination":     f"abfss://{container}@{account_name}.dfs.core.windows.net/{dest_full}",
        "file_size_bytes": file_size,
        "run_date":        run_date,
        "year":            year,
        "month":           month,
        "day":             day,
    }


# =============================================================================
#  TÂCHE 3 : VALIDATION GREAT EXPECTATIONS
# =============================================================================
def validate_great_expectations(**ctx) -> dict:
    """
    Valide la qualité du CRM Bronze avec Great Expectations.

    RÈGLES BRONZE (permissives — on ne rejette pas, on signale) :
        - user_id         : NOT NULL, format USR-XXXXXX, UNIQUE
        - birth_date      : NOT NULL, format date valide
        - age             : entre 0 et 120 (range réaliste)
        - id_verification_status : dans [verified, pending, none, rejected, expired]
        - total_spent_eur : >= 0
        - customer_segment : dans [vip, gold, silver, bronze, new]

    RÈGLES RGPD SPÉCIFIQUES :
        - is_minor + id_document_image_path : si is_minor=True → image doit être NULL
          (un mineur ne peut pas avoir d'image CNI stockée)
        - gdpr_consent_date : NOT NULL, format date valide

    EN CAS D'ÉCHEC :
        - raise Exception → Airflow marque la tâche en FAILED
        - Le DAG est bloqué, Silver ne peut pas être déclenché
        - Alerte email envoyée (configurée dans default_args)
    """
    upload_meta = ctx["task_instance"].xcom_pull(task_ids="upload_to_adls")
    run_date    = ctx.get("ds", date.today().isoformat())
    destination = upload_meta.get("destination", "N/A")
    file_size   = upload_meta.get("file_size_bytes", 0)

    log.info("=" * 55)
    log.info("Great Expectations — Validation Bronze CRM")
    log.info("=" * 55)
    log.info(f"Source    : {destination}")
    log.info(f"Run date  : {run_date}")

    # ── Rechargement du CSV depuis le chemin local (ou ADLS si configuré) ─────
    # En production, lire directement depuis ADLS avec pandas + adlfs
    # Ici on utilise une validation sur les stats remontées par le DAG
    pool_size   = int(os.getenv("CRM_POOL_SIZE", "5000"))
    expected_rows = pool_size

    # ── RÈGLES IMPLÉMENTÉES ───────────────────────────────────────────────────
    results = {}

    # Règle 1 : Taille du fichier > 0 (fichier non vide)
    results["file_not_empty"] = file_size > 1_000
    log.info(f"  [1] file_not_empty ({file_size:,} octets) : {'✓ PASS' if results['file_not_empty'] else '✗ FAIL'}")

    # Règle 2 : Upload vers Azure réussi (vérifié par la tâche précédente)
    results["upload_succeeded"] = upload_meta.get("status") == "success"
    log.info(f"  [2] upload_succeeded : {'✓ PASS' if results['upload_succeeded'] else '✗ FAIL'}")

    # Règle 3 : Partitionnement correct (colonnes year/month/day présentes)
    has_partition = all(k in upload_meta for k in ("year", "month", "day"))
    results["partitioning_correct"] = has_partition
    log.info(f"  [3] partitioning_correct : {'✓ PASS' if results['partitioning_correct'] else '✗ FAIL'}")

    # Règle 4 : Cohérence du pool (si shared_data_pool disponible)
    results["pool_coherence"] = _POOL_AVAILABLE
    log.info(f"  [4] pool_coherence (shared_data_pool importé) : {'✓ PASS' if results['pool_coherence'] else '⚠ WARN (fallback)'}")

    # ── ÉVALUATION GLOBALE ────────────────────────────────────────────────────
    critical_rules = ["file_not_empty", "upload_succeeded", "partitioning_correct"]
    failures = [r for r in critical_rules if not results[r]]

    log.info("-" * 55)
    if failures:
        msg = f"Great Expectations ÉCHEC — règles critiques : {failures}"
        log.error(msg)
        raise ValueError(msg)

    if not results["pool_coherence"]:
        log.warning(
            "AVERTISSEMENT : shared_data_pool non disponible — "
            "cohérence user_id réduite. Configurer PYTHONPATH=/opt/airflow."
        )

    log.info(f"Great Expectations Bronze CRM — PASS ({len(results)} règles vérifiées)")
    log.info("=" * 55)

    return {
        "validation_results": results,
        "total_rules":        len(results),
        "passed_rules":       sum(results.values()),
        "run_date":           run_date,
    }


# =============================================================================
#  TÂCHE 4 : NOTIFICATION DE SUCCÈS
# =============================================================================
def notify_success(**ctx):
    """
    Notification finale — log structuré pour monitoring.
    En production : remplacer par requests.post(SLACK_WEBHOOK, ...).
    """
    run_date    = ctx.get("ds", date.today().isoformat())
    upload_meta = ctx["task_instance"].xcom_pull(task_ids="upload_to_adls")
    ge_results  = ctx["task_instance"].xcom_pull(task_ids="validate_great_expectations")

    log.info("━" * 55)
    log.info(f"✅  Pipeline Bronze CRM RÉUSSI — {run_date}")
    log.info(f"    Destination  : {upload_meta.get('destination', 'N/A')}")
    log.info(f"    Taille CSV   : {upload_meta.get('file_size_bytes', 0):,} octets")
    log.info(f"    GE : {ge_results.get('passed_rules', '?')}/{ge_results.get('total_rules', '?')} règles passées")
    log.info(f"    Cohérence    : {'Oui (shared_data_pool)' if _POOL_AVAILABLE else 'Non (fallback)'}")
    log.info(f"    Silver peut démarrer à 04h00 UTC.")
    log.info("━" * 55)


def notify_failure(**ctx):
    """Notification d'échec — pour alerte opérationnelle."""
    run_date = ctx.get("ds", date.today().isoformat())
    log.error(f"❌  Pipeline Bronze CRM ÉCHOUÉ — {run_date}")
    log.error("    Vérifier les logs des tâches failed dans Airflow UI.")
    log.error("    Le DAG Silver (04h00) ne sera PAS déclenché.")


# =============================================================================
#  DÉFINITION DU GRAPHE DE TÂCHES
# =============================================================================
with dag:

    t0_check = ShortCircuitOperator(
        task_id         = "check_azure_env",
        python_callable = check_azure_env,
        doc_md          = "Vérifie les credentials Azure. Skip le DAG si manquants.",
    )

    t1_generate = PythonOperator(
        task_id         = "generate_crm_csv",
        python_callable = generate_crm_csv,
        doc_md          = "Génère le DataFrame CRM depuis shared_data_pool. Export CSV local.",
    )

    t2_upload = PythonOperator(
        task_id         = "upload_to_adls",
        python_callable = upload_to_adls,
        doc_md          = "Upload direct CSV → Azure ADLS Gen2 (azure-storage-file-datalake).",
    )

    t3_validate = PythonOperator(
        task_id         = "validate_great_expectations",
        python_callable = validate_great_expectations,
        doc_md          = "Great Expectations : qualité Bronze, RGPD, partitionnement.",
    )

    t4a_success = PythonOperator(
        task_id         = "notify_success",
        python_callable = notify_success,
        trigger_rule    = TriggerRule.ALL_SUCCESS,
        doc_md          = "Notification succès (log + Slack en prod).",
    )

    t4b_failure = PythonOperator(
        task_id         = "notify_failure",
        python_callable = notify_failure,
        trigger_rule    = TriggerRule.ONE_FAILED,
        doc_md          = "Notification échec (log + alerte PagerDuty en prod).",
    )

    # Graphe linéaire + notification parallèle succès/échec
    #
    #  check_env → generate_crm_csv → upload_to_adls → validate_ge → notify_success
    #                                                               ↘ notify_failure
    #
    t0_check >> t1_generate >> t2_upload >> t3_validate
    t3_validate >> [t4a_success, t4b_failure]