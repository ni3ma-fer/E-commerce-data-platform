# airflow/dags/dag_ml_retrain.py
"""
DAG de réentraînement hebdomadaire XGBoost.
 
STRATÉGIE CHAMPION / CHALLENGER :
    Chaque dimanche à 03h00 UTC :
    1. Entraîner un nouveau modèle Challenger sur les 30 derniers jours
    2. Évaluer sur un ensemble de validation récent (7 derniers jours)
    3. Si F1 Challenger > 0.90 → promouvoir en Production (archive le Champion)
    4. Sinon → log de l'échec, modèle Champion conservé
"""
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import subprocess, logging, os
log = logging.getLogger("dag_ml_retrain")
 
dag = DAG(
    dag_id            = "ml_fraud_weekly_retrain",
    description       = "Réentraînement XGBoost fraude — Champion/Challenger hebdo",
    default_args      = {
        "owner":       "ml-team-kivendtout",
        "retries":     1,
        "retry_delay": timedelta(minutes=30),
        "email_on_failure": True,
    },
    schedule_interval = "0 3 * * 0",   # Chaque dimanche 03h00 UTC
    start_date        = days_ago(1),
    catchup           = False,
    tags              = ["ml", "xgboost", "retrain", "finops"],
)
 
def run_training(**ctx):
    """Lance le script d'entraînement et capture les métriques."""
    result = subprocess.run(
        ["python", "/opt/airflow/ml/fraud_detection/train.py"],
        capture_output=True, text=True, timeout=3600
    )
    log.info(result.stdout[-5000:])
    if result.returncode != 0:
        log.error(result.stderr)
        raise Exception(f"Entraînement échoué (code {result.returncode})")
    log.info("Réentraînement terminé.")
 
def validate_champion(**ctx):
    """Vérifie qu'un modèle est bien en Production dans MLflow Registry."""
    import mlflow
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
    client = mlflow.tracking.MlflowClient()
    prod_models = client.get_latest_versions(
        "fraud-xgboost-champion", stages=["Production"]
    )
    if not prod_models:
        raise Exception(
            "Aucun modèle en Production après réentraînement ! "
            "Vérifier les métriques dans MLflow : http://localhost:5000"
        )
    log.info(f"Champion validé : version {prod_models[0].version}")
 
with dag:
    t1 = PythonOperator(task_id="run_training",      python_callable=run_training)
    t2 = PythonOperator(task_id="validate_champion", python_callable=validate_champion)
    t1 >> t2
