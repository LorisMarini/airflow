from skepsi.emails.agg import extract_email_aggs_cross_instance
from skepsi.emails.results import parallel_email_anomaly_cards_cross_instance
from skepsi.emails.results import load_email_results_cross_instance
from skepsi.emails.results import clean_temp_email_cross_instance

from skepsi.emails.reports import email_anomaly_ci_coverage_report

from skepsi.utils.etl import task_fail_slack_alert
from skepsi.utils.imports import *


# ------------------ INSTANTIATE DAG ------------------

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 6),
    'email': ['loris@autopilothq.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=3),
    'on_failure_callback': task_fail_slack_alert
}

# Dag Declaration
dag = DAG('email_outliers_ci', catchup=True, max_active_runs=1, default_args=default_args, schedule_interval='@daily')

# Retrieve execution date from template
# This will be a datetime only during a DAG run
context = {"execution_date": "{{ execution_date }}"}


with dag:

    # -------------- AGG SENSOR -----------------

    # Wait for email results to be available
    ETS_aggs90dci = ExternalTaskSensor(external_dag_id="email_aggs90d",
                                       external_task_id="agg90dci_stop",
                                       task_id="wait_aggs90dci",
                                       timeout=72000)

    # Wait for email results to be available
    ETS_email_30 = ExternalTaskSensor(external_dag_id="email_aggs30d",
                                      external_task_id="agg30d_stop",
                                      task_id="wait_email_aggs30d",
                                      timeout=72000)

    # -------------- WORK -----------------

    # Extract input from storage
    f = extract_email_aggs_cross_instance
    PO_extract_email_aggs_cross_instance = PythonOperator(task_id=f.__name__, python_callable=f,
                                                          provide_context=True, trigger_rule="all_success")

    # Generate insights
    f = parallel_email_anomaly_cards_cross_instance
    PO_parallel_email_anomaly_cards_cross_instance = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    # Load results to storage
    f = load_email_results_cross_instance
    PO_load_email_results_cross_instance = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    # Make report and load to storage
    f = email_anomaly_ci_coverage_report
    S_email_anomaly_ci_coverage_report = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    # Clean local copies of assets
    f = clean_temp_email_cross_instance
    PO_clean_temp = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    # ----------- DEPENDENCIES ---------------

    [ETS_aggs90dci, ETS_email_30] >> PO_extract_email_aggs_cross_instance >> \
        PO_parallel_email_anomaly_cards_cross_instance >> \
        PO_load_email_results_cross_instance >> S_email_anomaly_ci_coverage_report >> PO_clean_temp
