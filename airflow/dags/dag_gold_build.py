# airflow/dags/dag_gold_build.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Configuration
# Note : Assure-toi que ce chemin correspond bien à ton installation (Docker ou local WSL)
DBT_DIR    = '/opt/airflow/dbt'
# On utilise la cible dev_local pour pointer sur DuckDB (approche FinOps)
DBT_OPTS   = '--profiles-dir /opt/airflow/dbt --target dev_local'

default_args = {
    'owner': 'data-team-kivendtout',
    'depends_on_past': True,    # Gold J dépend du succès de Gold J-1
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'email_on_failure': True,
}

dag = DAG(
    dag_id            = 'gold_daily_build',
    description       = 'Silver → Gold via dbt-core avec BashOperator (FinOps)',
    default_args      = default_args,
    schedule_interval = '0 5 * * *',   # 05h00 UTC — après Silver (04h00)
    start_date        = days_ago(1),
    catchup           = False,
    tags              = ['gold','dbt','finops','kivendtout'],
)

# Fonction génératrice de commande Bash pour éviter les conflits `.format()` avec le JSON
# Elle gère proprement l'injection de la date Airflow {{ ds }} dans dbt
def build_cmd(cmd):
    return f"cd {DBT_DIR} && dbt {cmd} {DBT_OPTS} --vars '{{\"run_date\": \"{{{{ ds }}}}\"}}'"

with dag:
    
    t_fresh = BashOperator(
        task_id='dbt_source_freshness',
        bash_command=build_cmd('source freshness')
    )

    t_dims = BashOperator(
        task_id='dbt_run_dimensions',
        bash_command=build_cmd('run --select tag:dimension')
    )

    t_facts = BashOperator(
        task_id='dbt_run_facts',
        bash_command=build_cmd('run --select tag:fact')
    )

    t_agg = BashOperator(
        task_id='dbt_run_aggregations',
        # Exécute à la fois les KPI et le Feature Store pour le Machine Learning
        bash_command=build_cmd('run --select tag:kpi tag:feature-store')
    )

    t_tests = BashOperator(
        task_id='dbt_test_gold',
        bash_command=build_cmd('test --select tag:gold')
    )

    t_docs = BashOperator(
        task_id='dbt_docs_generate',
        bash_command=build_cmd('docs generate')
    )

    # Graphe strict : fraîcheur des sources → dimensions → faits → agrégations → tests → documentation
    t_fresh >> t_dims >> t_facts >> t_agg >> t_tests >> t_docs