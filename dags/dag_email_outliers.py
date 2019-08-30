from skepsi.emails.agg import extract_email_aggs
from skepsi.emails.results import parallel_email_anomaly_cards
from skepsi.emails.results import load_email_results
from skepsi.emails.results import clean_temp_email

from skepsi.utils.etl import task_fail_slack_alert
from skepsi.emails.reports import email_anomaly_coverage_report

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
dag = DAG('email_outliers', catchup=True, max_active_runs=1, default_args=default_args, schedule_interval='@daily')

# Retrieve execution date from template
# This will be a datetime only during a DAG run
context = {"execution_date": "{{ execution_date }}"}

with dag:

    # -------------- DUMMY -----------------

    DO_stop = DummyOperator(task_id="email_results_stop", trigger_rule="one_success")

    # -------------- SENSORS -----------------

    # Wait for email results to be available
    ETS_email_90 = ExternalTaskSensor(external_dag_id="email_aggs90d",
                                      external_task_id="agg90dci_stop",
                                      task_id="wait_email_aggs90d",
                                      timeout=72000)

    # Wait for email results to be available
    ETS_email_30 = ExternalTaskSensor(external_dag_id="email_aggs30d",
                                      external_task_id="agg30d_stop",
                                      task_id="wait_email_aggs30d",
                                      timeout=72000)

    # -------------- WORK -----------------

    f = extract_email_aggs
    PO_extract_email_aggs = PythonOperator(task_id=f.__name__, python_callable=f, trigger_rule="all_success",
                                           provide_context=True, op_kwargs={"agg_number": 90})

    f = parallel_email_anomaly_cards
    PO_generate_cards = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    f = load_email_results
    PO_load_email_results = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    f = email_anomaly_coverage_report
    PO_report = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    f = clean_temp_email
    PO_clean_temp_email = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    # ----------- DEPENDENCIES ---------------

    [ETS_email_90, ETS_email_30] >> PO_extract_email_aggs >> PO_generate_cards >> \
        PO_load_email_results >> PO_report >> PO_clean_temp_email >> DO_stop
