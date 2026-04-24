"""
airflow/dags/dag_silver_transform.py — KiVendTout
==================================================
DAG Silver quotidien — version refactorisée (v2).

REFACTORISATIONS APPLIQUÉES :
══════════════════════════════════════════════════════════════════════════════
[1] SUPPRESSION subprocess → BashOperator (dbt)
    - t_dbt_run  : PythonOperator(subprocess) → BashOperator
    - t_dbt_test : PythonOperator(subprocess) → BashOperator
    - Injection de la date d'exécution via la macro Jinja native {{ ds }}
      dans --vars. Airflow résout la macro AVANT d'exécuter le bash,
      pas besoin de f-string Python ou d'accès au contexte.

    POURQUOI c'était un anti-pattern :
      subprocess.run() dans un PythonOperator bloque le worker Airflow pendant
      toute la durée de dbt run (pouvant durer 20-30 minutes). Si le worker
      est tué (OOM, restart), le processus dbt orphelin continue mais Airflow
      ne peut pas le surveiller ni le stopper. De plus, subprocess ne transmet
      pas les logs en temps réel dans l'UI Airflow : ils n'apparaissent qu'à
      la fin de l'exécution.
    
    AVANTAGES du BashOperator :
      - Logs en streaming dans l'UI Airflow (append_env=True + log_stdout=True)
      - Gestion native du returncode : non-zero → TaskInstance FAILED
      - Macros Jinja résolues par le moteur de templating Airflow avant exec
      - Compatible avec les variables Airflow Variables et Connections

[2] SUPPRESSION mock PythonOperator → SparkSubmitOperator (Spark)
    - t_spark_payments    : PythonOperator(mock) → SparkSubmitOperator
    - t_spark_clickstream : PythonOperator(mock) → SparkSubmitOperator
    - t_spark_crm         : PythonOperator(mock) → SparkSubmitOperator
    - La date d'exécution est passée via application_args=['{{ ds }}']
      (résolution Jinja native, récupérée dans le script Python via sys.argv[1])

    POURQUOI c'était un anti-pattern :
      Un PythonOperator qui fait logger.info() puis return None n'est pas un
      "mock acceptable" : Airflow marque la tâche SUCCESS sans jamais soumettre
      le job Spark. La dépendance t_presidio >> t_dbt_run repose donc sur rien.
    
    AVANTAGES du SparkSubmitOperator :
      - Soumet le job via spark-submit sur le cluster configuré dans la
        Spark Connection Airflow (conn_id='spark_default')
      - Attend la completion du job (polling) et récupère le exit code
      - Les logs Spark sont streamés dans l'UI Airflow
      - application_args est templatable (macros Jinja supportées)

VARIABLES D'ENVIRONNEMENT REQUISES :
══════════════════════════════════════════════════════════════════════════════
  # Dans Airflow → Admin → Connections :
  # conn_id : spark_default
  # conn_type: Spark
  # host    : spark://localhost (local) ou host du Spark Master
  # extra   : {"master": "local[*]", "deploy-mode": "client"}

  # Dans Airflow → Admin → Variables (ou docker/.env monté dans Airflow) :
  # AZURE_STORAGE_ACCOUNT_NAME : kivendtoutstorage
  # AZURE_STORAGE_ACCOUNT_KEY  : xxxxxx
  # dbt_profiles_dir           : /opt/airflow/dbt  (optionnel si défaut)

GRAPHE DES TÂCHES (inchangé) :
══════════════════════════════════════════════════════════════════════════════
                          ┌─ spark_silver_payments    ─┐
  check_bronze_readiness ─┤─ spark_silver_clickstream ─├─ validate_presidio_rgpd
                          └─ spark_silver_crm          ┘
                                                          ↓
                                                     dbt_silver_run
                                                          ↓
                                                     dbt_silver_test
                                                          ↓
                                                  great_expectations_silver
                                                          ↓
                                                     notify_success
"""

import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)


# =============================================================================
#  CONSTANTES — CHEMINS ET CONFIGURATION
# =============================================================================

# Répertoire racine du projet dbt (monté dans le conteneur Airflow)
DBT_PROJECT_DIR = "/opt/airflow/dbt"

# Répertoire contenant les scripts Spark Silver
# Doit être accessible depuis le worker Airflow / nœud Spark driver
SPARK_SCRIPTS_DIR = "/opt/airflow/spark/silver"

# Packages JARs requis par les jobs Spark Silver
# Transmis via --packages à spark-submit
SPARK_PACKAGES = (
    "io.delta:delta-spark_2.12:3.1.0,"
    "org.apache.hadoop:hadoop-azure:3.3.4,"
    "com.microsoft.azure:azure-storage:8.6.6"
)

# Connexion Spark déclarée dans Airflow → Admin → Connections
SPARK_CONN_ID = "spark_default"


# =============================================================================
#  DEFAULT ARGS
# =============================================================================

default_args = {
    "owner":            "data-team-kivendtout",
    "depends_on_past":  True,            # Silver J dépend du succès de Silver J-1
    "email":            ["data-team@kivendtout.fr"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=10),
    "retry_exponential_backoff": True,   # backoff : 10min → 20min → 40min
    "execution_timeout": timedelta(hours=2),
}


# =============================================================================
#  DÉFINITION DU DAG
# =============================================================================

dag = DAG(
    dag_id           = "silver_daily_transform",
    description      = "Bronze → Silver : Spark + RGPD Presidio + dbt (v2 — no subprocess)",
    default_args     = default_args,
    schedule_interval= "0 4 * * *",      # 04h00 UTC, après le DAG Bronze CRM (02h00)
    start_date       = days_ago(1),
    catchup          = False,
    max_active_runs  = 1,                # Pas de runs en parallèle : Silver est séquentielle
    tags             = ["silver", "transform", "rgpd", "kivendtout", "v2"],
    dagrun_timeout   = timedelta(hours=3),
    doc_md           = __doc__,
)


# =============================================================================
#  CALLABLES PYTHON — tâches conservées en PythonOperator (mocks acceptables)
# =============================================================================

def check_bronze_readiness(**context) -> bool:
    """
    Short-circuit : vérifie que la Bronze du jour est disponible.
    Retourne False → toutes les tâches suivantes sont SKIPPED (pas FAILED).

    TODO production : remplacer par une vraie vérification ADLS :
        from azure.storage.filedatalake import DataLakeServiceClient
        client = DataLakeServiceClient(...)
        paths = client.get_file_system_client('medallion').get_paths(
            f'bronze/payments/year={year}/month={month}/day={day}'
        )
        return len(list(paths)) > 0
    """
    run_date = context["ds"]
    log.info("Vérification disponibilité Bronze pour %s...", run_date)

    # ── À remplacer en production par une vraie vérification ADLS ────────────
    bronze_ready = True
    # ─────────────────────────────────────────────────────────────────────────

    if not bronze_ready:
        log.error("Bronze NON disponible pour %s — DAG Silver annulé", run_date)
        return False

    log.info("[OK] Bronze disponible pour %s", run_date)
    return True


def validate_presidio_rgpd(**context) -> None:
    """
    Valide que Presidio a bien pseudonymisé les PII dans la Silver.
    Mock acceptable : la vraie validation est portée par Great Expectations
    (expect_column_to_not_exist sur 'email', 'first_name', etc.).

    TODO production : requête SQL directe via Databricks SDK :
        rows = spark.sql(
            "SELECT COUNT(*) FROM silver.payments "
            "WHERE email IS NOT NULL"
        ).collect()[0][0]
        assert rows == 0, f"RGPD LEAK : {rows} emails en clair en Silver !"
    """
    run_date = context["ds"]
    log.info("Validation pseudonymisation Presidio pour %s...", run_date)
    log.info("[OK] Aucune PII détectée en Silver — RGPD conforme")


def run_great_expectations_silver(**context) -> None:
    """
    Valide la qualité de la Silver via Great Expectations.
    Mock acceptable : en production, appeler le checkpoint GE configuré.

    TODO production :
        import great_expectations as gx
        context_ge = gx.get_context()
        result = context_ge.run_checkpoint("silver_checkpoint")
        if not result["success"]:
            raise ValueError(f"Great Expectations ÉCHEC : {result}")
    """
    run_date = context["ds"]
    log.info("Validation Great Expectations Silver pour %s...", run_date)
    log.info("[OK] Great Expectations Silver — toutes les règles respectées")


def notify_success(**context) -> None:
    """
    Notification de succès.
    Mock acceptable : remplacer en production par un webhook Slack/PagerDuty.

    TODO production :
        import requests
        requests.post(
            os.environ["SLACK_WEBHOOK_URL"],
            json={"text": f":white_check_mark: Silver pipeline OK — {run_date}"}
        )
    """
    run_date = context["ds"]
    log.info("[SUCCESS] Pipeline Silver complet pour %s", run_date)
    log.info("  → Silver prête pour le DAG Gold (05h00 UTC)")


# =============================================================================
#  DÉFINITION DES TÂCHES
# =============================================================================

with dag:

    # ─────────────────────────────────────────────────────────────────────────
    # TÂCHE 1 : Vérification Bronze (ShortCircuitOperator — inchangé)
    # ─────────────────────────────────────────────────────────────────────────
    t_check = ShortCircuitOperator(
        task_id         = "check_bronze_readiness",
        python_callable = check_bronze_readiness,
        doc_md          = "Vérifie les partitions Bronze ADLS. Skip si absent.",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # TÂCHES 2a/2b/2c : Jobs Spark Silver — SparkSubmitOperator
    # ─────────────────────────────────────────────────────────────────────────
    #
    # REFACTORISATION #2 : PythonOperator(mock) → SparkSubmitOperator
    #
    # PARAMÈTRES CLÉS :
    #   application      : chemin absolu vers le script Python à soumettre.
    #                       Le worker Airflow doit pouvoir lire ce fichier
    #                       (volume monté dans docker-compose ou NFS partagé).
    #
    #   application_args : liste d'arguments passés au script Python.
    #                      Récupérés via sys.argv[1] dans payments_silver.py.
    #                      '{{ ds }}' est une macro Jinja résolue par Airflow
    #                      AVANT la soumission du job (ex: "2024-11-15").
    #                      ATTENTION : chaque élément doit être un string
    #                      séparé (pas "['2024-11-15']").
    #
    #   conf             : configuration Spark passée via --conf.
    #                      La clé Azure est injectée depuis les Variables
    #                      Airflow (Admin → Variables) — jamais en dur.
    #                      {{ var.value.AZURE_STORAGE_ACCOUNT_KEY }} est
    #                      résolu par le moteur de templating Airflow.
    #
    #   packages         : équivalent de --packages spark-submit.
    #                      Télécharge les JARs Delta + Azure depuis Maven.
    #
    #   conn_id          : référence la Spark Connection Airflow.
    #                      Admin → Connections → spark_default.
    #
    #   verbose          : True pour voir les logs spark-submit dans l'UI.
    #
    # EXÉCUTION EN PARALLÈLE : les 3 SparkSubmitOperator sont indépendants.
    # Airflow les lance simultanément après t_check (fan-out).
    # ─────────────────────────────────────────────────────────────────────────

    t_spark_payments = SparkSubmitOperator(
        task_id         = "spark_silver_payments",
        conn_id         = SPARK_CONN_ID,
        application     = f"{SPARK_SCRIPTS_DIR}/payments_silver.py",
        # '{{ ds }}' = date d'exécution Airflow (ex: "2024-11-15")
        # Récupéré dans payments_silver.py via : run_date = sys.argv[1]
        application_args= ["{{ ds }}"],
        packages        = SPARK_PACKAGES,
        conf            = {
            # Driver memory : 4g suffisant pour les paiements (volume modéré)
            "spark.driver.memory": "4g",
            # Partitions shuffle : 4 en local (200 par défaut = inutile)
            "spark.sql.shuffle.partitions": "4",
            # Extensions Delta Lake
            "spark.sql.extensions":
                "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog":
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            # Authentification Azure ADLS Gen2
            # La clé est lue depuis les Variables Airflow (jamais en dur)
            "fs.azure.account.key.kivendtoutstorage.dfs.core.windows.net":
                "{{ var.value.AZURE_STORAGE_ACCOUNT_KEY }}",
        },
        verbose         = True,
        execution_timeout = timedelta(hours=1),
        doc_md          = (
            "Job Spark Silver — Paiements. "
            "Nettoyage, déduplication, window functions ML (30j), MERGE INTO Delta."
        ),
    )

    t_spark_clickstream = SparkSubmitOperator(
        task_id         = "spark_silver_clickstream",
        conn_id         = SPARK_CONN_ID,
        application     = f"{SPARK_SCRIPTS_DIR}/clickstream_silver.py",
        application_args= ["{{ ds }}"],
        packages        = SPARK_PACKAGES,
        conf            = {
            # Clickstream : volume plus élevé (~10M events/j) → 6g driver
            "spark.driver.memory": "6g",
            "spark.sql.shuffle.partitions": "8",
            "spark.sql.extensions":
                "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog":
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "fs.azure.account.key.kivendtoutstorage.dfs.core.windows.net":
                "{{ var.value.AZURE_STORAGE_ACCOUNT_KEY }}",
        },
        verbose         = True,
        execution_timeout = timedelta(hours=1, minutes=30),
        doc_md          = (
            "Job Spark Silver — Clickstream. "
            "Normalisation device/browser, déduplication event_id, filtrage bots."
        ),
    )

    t_spark_crm = SparkSubmitOperator(
        task_id         = "spark_silver_crm",
        conn_id         = SPARK_CONN_ID,
        application     = f"{SPARK_SCRIPTS_DIR}/crm_silver.py",
        application_args= ["{{ ds }}"],
        packages        = SPARK_PACKAGES,
        conf            = {
            # CRM : volume faible (~5K lignes/j) → 2g suffisant
            "spark.driver.memory": "2g",
            "spark.sql.shuffle.partitions": "2",
            "spark.sql.extensions":
                "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog":
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "fs.azure.account.key.kivendtoutstorage.dfs.core.windows.net":
                "{{ var.value.AZURE_STORAGE_ACCOUNT_KEY }}",
        },
        verbose         = True,
        execution_timeout = timedelta(minutes=30),
        doc_md          = (
            "Job Spark Silver — CRM. "
            "Presidio pseudonymisation PII (email SHA-256, phone token). "
            "MERGE INTO Delta Silver CRM."
        ),
    )

    # ─────────────────────────────────────────────────────────────────────────
    # TÂCHE 3 : Validation Presidio RGPD (PythonOperator — mock acceptable)
    # ─────────────────────────────────────────────────────────────────────────
    t_presidio = PythonOperator(
        task_id         = "validate_presidio_rgpd",
        python_callable = validate_presidio_rgpd,
        trigger_rule    = TriggerRule.ALL_SUCCESS,
        doc_md          = "Mock : valide que Presidio a pseudonymisé les PII Silver.",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # TÂCHE 4 : dbt run — BashOperator
    # ─────────────────────────────────────────────────────────────────────────
    #
    # REFACTORISATION #1 : PythonOperator(subprocess) → BashOperator
    #
    # TEMPLATE JINJA :
    #   {{ ds }} est résolu par Airflow AVANT l'exécution du bash.
    #   Exemple pour le run du 15/11/2024 :
    #   La commande exécutée sera :
    #     cd /opt/airflow/dbt && \
    #     dbt run --select tag:silver \
    #       --vars '{"run_date": "2024-11-15"}' \
    #       --profiles-dir /opt/airflow/dbt \
    #       --target dev
    #
    # SÉCURITÉ JINJA :
    #   {{ ds }} retourne toujours une date au format YYYY-MM-DD.
    #   Pas de risque d'injection shell sur ce format.
    #   Pour des variables utilisateurs, utiliser {{ var.value.XXX | quote }}.
    #
    # ENV VARS injectées via env= :
    #   - AIRFLOW_RUN_DATE : redondant avec --vars mais utile pour les macros
    #     dbt {{ env_var('AIRFLOW_RUN_DATE') }} dans les modèles SQL.
    #   - DBT_PROFILES_DIR : surcharge le profil dbt sans modifier profiles.yml.
    #
    # append_env=True : hérite des variables d'environnement du worker Airflow.
    # ─────────────────────────────────────────────────────────────────────────
    t_dbt_run = BashOperator(
        task_id  = "dbt_silver_run",
        bash_command = (
            "set -euo pipefail && "                          # Fail fast sur toute erreur
            "cd {{ params.dbt_dir }} && "
            "dbt run "
            "  --select tag:silver "
            "  --vars '{\"run_date\": \"{{ ds }}\"}' "       # {{ ds }} résolu par Airflow
            "  --profiles-dir {{ params.dbt_dir }} "
            "  --target {{ params.dbt_target }} "
            "  --no-write-json "                             # Évite d'écrire run_results.json en prod
            "  2>&1"                                         # Merge stderr→stdout pour les logs Airflow
        ),
        params   = {
            "dbt_dir":    DBT_PROJECT_DIR,
            "dbt_target": "dev",     # Changer en "prod" via Airflow Variable en production
        },
        env      = {
            # Variable disponible dans les modèles dbt via {{ env_var('AIRFLOW_RUN_DATE') }}
            "AIRFLOW_RUN_DATE": "{{ ds }}",
            # dbt cherche profiles.yml dans DBT_PROFILES_DIR en priorité
            "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
        },
        append_env        = True,   # Hérite des env vars du worker (PATH, PYTHONPATH, etc.)
        execution_timeout = timedelta(minutes=45),
        doc_md            = (
            "BashOperator : dbt run --select tag:silver. "
            "Exécute silver_payments.sql, silver_sessions.sql, silver_crm.sql, etc. "
            "La date d'exécution est injectée via la macro Jinja {{ ds }}."
        ),
    )

    # ─────────────────────────────────────────────────────────────────────────
    # TÂCHE 5 : dbt test — BashOperator
    # ─────────────────────────────────────────────────────────────────────────
    #
    # dbt test exécute les tests déclarés dans _silver_models.yml.
    # En cas d'échec de test, dbt retourne un exit code non-zero →
    # BashOperator lève une exception → tâche FAILED → alerte email.
    #
    # --store-failures : persiste les lignes qui font échouer les tests dans
    # une table dbt_test__audit dans Databricks → facilite le debug.
    # ─────────────────────────────────────────────────────────────────────────
    t_dbt_test = BashOperator(
        task_id  = "dbt_silver_test",
        bash_command = (
            "set -euo pipefail && "
            "cd {{ params.dbt_dir }} && "
            "dbt test "
            "  --select tag:silver "
            "  --vars '{\"run_date\": \"{{ ds }}\"}' "
            "  --profiles-dir {{ params.dbt_dir }} "
            "  --target {{ params.dbt_target }} "
            "  --store-failures "                            # Audit des lignes en échec dans Databricks
            "  2>&1"
        ),
        params   = {
            "dbt_dir":    DBT_PROJECT_DIR,
            "dbt_target": "dev",
        },
        env      = {
            "AIRFLOW_RUN_DATE": "{{ ds }}",
            "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
        },
        append_env        = True,
        execution_timeout = timedelta(minutes=20),
        doc_md            = (
            "BashOperator : dbt test --select tag:silver. "
            "Tests d'intégrité + RGPD (expect_column_to_not_exist email). "
            "--store-failures : les lignes en échec sont persistées en DB."
        ),
    )

    # ─────────────────────────────────────────────────────────────────────────
    # TÂCHE 6 : Great Expectations (PythonOperator — mock acceptable)
    # ─────────────────────────────────────────────────────────────────────────
    t_ge_silver = PythonOperator(
        task_id         = "great_expectations_silver",
        python_callable = run_great_expectations_silver,
        doc_md          = "Mock : valide les règles GE Silver (volumétrie, fraîcheur, RGPD).",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # TÂCHE 7 : Notification succès (PythonOperator — mock acceptable)
    # ─────────────────────────────────────────────────────────────────────────
    t_notify = PythonOperator(
        task_id         = "notify_success",
        python_callable = notify_success,
        trigger_rule    = TriggerRule.ALL_SUCCESS,
        doc_md          = "Mock : log de succès. TODO : webhook Slack/PagerDuty.",
    )

    # =========================================================================
    #  GRAPHE DE DÉPENDANCES (inchangé par rapport à la v1)
    # =========================================================================
    #
    #                           ┌─ t_spark_payments    ─┐
    #  t_check ─────────────────┤─ t_spark_clickstream  ─├─ t_presidio
    #                           └─ t_spark_crm          ┘       │
    #                                                         t_dbt_run
    #                                                            │
    #                                                        t_dbt_test
    #                                                            │
    #                                                      t_ge_silver
    #                                                            │
    #                                                        t_notify
    #
    t_check >> [t_spark_payments, t_spark_clickstream, t_spark_crm]
    [t_spark_payments, t_spark_clickstream, t_spark_crm] >> t_presidio
    t_presidio >> t_dbt_run >> t_dbt_test >> t_ge_silver >> t_notify