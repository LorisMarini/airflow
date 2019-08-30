from skepsi.emails.agg import generate_email_aggsnd
from skepsi.emails.agg import extract_many_email_aggs1d
from skepsi.emails.agg import generate_email_aggsnd_ci
from skepsi.emails.agg import generate_email_aggsnd_report

from skepsi.utils.etl import task_fail_slack_alert
from skepsi.utils.imports import *

# ------------------ INSTANTIATE DAG ------------------

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 3, 1),
    'email': ['loris@autopilothq.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=3),
    'on_failure_callback': task_fail_slack_alert
}

# Dag Declaration
dag = DAG('email_aggs30d', catchup=True, max_active_runs=1, default_args=default_args, schedule_interval='@daily')

# Retrieve execution date from template
# This will be a datetime only during a DAG run
context = {"execution_date": "{{ execution_date }}"}


with dag:

    # -------------- SENSORS -----------------

    # Wait for agg1d
    ETS_agg1d = ExternalTaskSensor(external_dag_id="email_aggs1d",
                                   external_task_id="agg1d_stop",
                                   task_id="wait_agg1d",
                                   timeout=72000)

    # -------------- WORK -----------------

    f = extract_many_email_aggs1d
    PO_extract_email_aggs = PythonOperator(task_id=f.__name__, python_callable=f,
                                           provide_context=True, op_kwargs={"days": 30})

    # Prepare individual 30day aggregates
    f = generate_email_aggsnd
    PO_generate_email_aggs30d = PythonOperator(task_id=f.__name__, python_callable=f,
                                               provide_context=True, op_kwargs={"days": 30})

    # Prepare 30day aggregate for cross-instance anomalies
    f = generate_email_aggsnd_ci
    PO_generate_email_aggs30d_ci = PythonOperator(task_id=f.__name__, python_callable=f,
                                                  provide_context=True, op_kwargs={"days": 30})

    f = generate_email_aggsnd_report
    PO_generate_email_aggs30d_report = PythonOperator(task_id=f.__name__, python_callable=f,
                                                      provide_context=True, op_kwargs={"days": 30})

    DO_agg30d_stop = DummyOperator(task_id="agg30d_stop", trigger_rule="one_success")

    # ----------- ALL DEPENDENCIES ---------------

    ETS_agg1d >> PO_extract_email_aggs >> PO_generate_email_aggs30d >> PO_generate_email_aggs30d_ci >> \
        PO_generate_email_aggs30d_report >> DO_agg30d_stop

