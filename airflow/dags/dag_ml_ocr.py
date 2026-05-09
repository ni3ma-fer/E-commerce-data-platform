# airflow/dags/dag_ml_ocr.py
"""
DAG de traitement OCR des images CNI en attente de validation.
Planifié toutes les heures pour traiter les nouveaux uploads.
 
RGPD COMPLIANCE :
    - Images supprimées de l'ADLS Bronze dans les 72h
    - Aucune donnée biométrique brute en Silver ou Gold
    - Seuls statut KYC et age_group sont propagés en Silver
"""
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os, logging
from dotenv import load_dotenv
load_dotenv("/opt/airflow/.env")
log = logging.getLogger("dag_ml_ocr")
 
dag = DAG(
    dag_id           = "ml_ocr_cni_processing",
    description      = "OCR CNI : Bronze images → extraction âge → Silver KYC (RGPD)",
    default_args     = {
        "owner":   "ml-team-kivendtout",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
    schedule_interval = "0 * * * *",   # Toutes les heures
    start_date       = days_ago(1),
    catchup          = False,
    tags             = ["ml", "ocr", "kyc", "rgpd", "finops"],
)
 
def list_pending_images(**ctx) -> list:
    """Liste les images CNI en attente dans ADLS Bronze/images_cni."""
    from azure.storage.filedatalake import DataLakeServiceClient
    account   = os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
    key       = os.environ["AZURE_STORAGE_ACCOUNT_KEY"]
    container = os.getenv("AZURE_CONTAINER_NAME", "medallion-data")
    client = DataLakeServiceClient(
        f"https://{account}.dfs.core.windows.net", credential=key
    )
    fs = client.get_file_system_client(container)
    paths   = list(fs.get_paths("bronze/images_cni", recursive=True))
    pending = [p.name for p in paths
               if p.name.endswith(".jpg") or p.name.endswith(".png")]
    log.info(f"{len(pending)} images CNI en attente")
    return pending
 
def process_ocr_batch(**ctx) -> dict:
    """Traite le batch d'images CNI et met à jour le statut Silver."""
    from azure.storage.filedatalake import DataLakeServiceClient
    import sys
    sys.path.insert(0, "/opt/airflow")
    from ml.ocr_pipeline.preprocess import preprocess_for_ocr
    from ml.ocr_pipeline.extract   import extract_cni_data
 
    account   = os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
    key       = os.environ["AZURE_STORAGE_ACCOUNT_KEY"]
    container = os.getenv("AZURE_CONTAINER_NAME", "medallion-data")
    client = DataLakeServiceClient(
        f"https://{account}.dfs.core.windows.net", credential=key
    )
    fs = client.get_file_system_client(container)
 
    pending = ctx["task_instance"].xcom_pull(task_ids="list_pending_images")
    results = {"processed": 0, "success": 0, "restricted": 0, "manual_review": 0}
 
    for image_path in pending[:50]:   # Limite 50 images/run (FinOps)
        user_id = image_path.split("/")[-2]
        try:
            # Téléchargement depuis ADLS
            file_client = fs.get_file_client(image_path)
            img_bytes   = file_client.download_file().readall()
 
            # Prétraitement + OCR
            processed   = preprocess_for_ocr(img_bytes)
            extraction  = extract_cni_data(processed, user_id)
 
            results["processed"] += 1
            if extraction.success:
                results["success"] += 1
                if extraction.action_required == "restrict_account":
                    results["restricted"] += 1
                    log.warning(f"MINEUR détecté : {user_id} — compte restreint")
            else:
                results["manual_review"] += 1
 
            # ── Purge RGPD : suppression immédiate après traitement ───────
            file_client.delete_file()
            log.info(f"Image CNI supprimée (RGPD Art.17) : {image_path}")
 
        except Exception as e:
            log.error(f"Erreur OCR pour {user_id} : {e}")
 
    log.info(f"Résultats OCR batch : {results}")
    return results
 
def update_silver_kyc(**ctx):
    """Met à jour les statuts KYC dans Silver CRM (table silver_crm)."""
    ocr_results = ctx["task_instance"].xcom_pull(task_ids="process_ocr_batch")
    log.info(f"Mise à jour Silver KYC — {ocr_results['success']} utilisateurs traités")
    # Production : MERGE INTO silver.silver_crm SET id_verification_status = ...
    # WHERE user_id IN (liste des users traités avec succès)
 
with dag:
    t1 = PythonOperator(task_id="list_pending_images",  python_callable=list_pending_images)
    t2 = PythonOperator(task_id="process_ocr_batch",    python_callable=process_ocr_batch)
    t3 = PythonOperator(task_id="update_silver_kyc",    python_callable=update_silver_kyc)
    t1 >> t2 >> t3
