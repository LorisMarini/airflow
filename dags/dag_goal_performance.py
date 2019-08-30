from skepsi.goals.initialize_goals import initialize_goal_pipeline_files
from skepsi.goals.query_kelp import parallel_extract_journey_aggs
from skepsi.goals.query_kelp import archive_load_goal_performance_detailed
from skepsi.goals.query_kelp import calculate_goal_performance_summary
from skepsi.goals.query_kelp import clean_goal_temps

from skepsi.goals.results import goal_cards_from_summary
from skepsi.goals.results import archive_load_goal_cards
from skepsi.goals.report import create_goal_summary_report

from skepsi.utils.etl import task_fail_slack_alert
from skepsi.utils.utils import *


# ------------------ INSTANTIATE DAG ------------------

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 15),
    'email': ['loris@autopilothq.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=3),
    'on_failure_callback': task_fail_slack_alert
}

# Dag Declaration
dag = DAG('goal_performance', catchup=True, max_active_runs=1, default_args=default_args, schedule_interval='@daily')

# Retrieve execution date from template
# This will be a datetime only during a DAG run
context = {"execution_date": "{{ execution_date }}"}


with dag:

    # Wait for record DAG
    ETS_subscriptions = ExternalTaskSensor(external_dag_id="subscriptions",
                                           external_task_id="sub_stop",
                                           task_id="wait_subs",
                                           timeout=72000)

    # --------------- LOGIC  ----------------

    f = initialize_goal_pipeline_files
    PO_initialize_goal_pipeline_files = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    f = parallel_extract_journey_aggs
    PO_parallel_extract = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    f = archive_load_goal_performance_detailed
    PO_archive_load_goal_performance_detailed = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    f = calculate_goal_performance_summary
    PO_performance_overview = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    f = create_goal_summary_report
    PO_create_goal_summary_report = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    f = goal_cards_from_summary
    PO_goal_cards_from_summary = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    f = archive_load_goal_cards
    PO_archive_load_goal_cards = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    f = clean_goal_temps
    PO_clean = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    # -------------- DEPENDENCIES -----------------

    ETS_subscriptions >> PO_initialize_goal_pipeline_files >> \
        PO_parallel_extract >> PO_archive_load_goal_performance_detailed >> \
        PO_performance_overview >> PO_create_goal_summary_report >> PO_goal_cards_from_summary >> \
        PO_archive_load_goal_cards >> PO_clean
