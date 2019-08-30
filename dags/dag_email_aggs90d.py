from skepsi.emails.agg import extract_email_aggs
from skepsi.emails.agg import generate_email_aggsnd
from skepsi.emails.agg import extract_many_email_aggs1d
from skepsi.emails.agg import generate_email_aggsnd_ci
from skepsi.emails.agg import generate_email_aggsnd_report
from skepsi.emails.agg import clean_emaill_aggsnd

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
dag = DAG('email_aggs90d', catchup=True, max_active_runs=1, default_args=default_args, schedule_interval='@daily')

# Retrieve execution date from template
# This will be a datetime only during a DAG run
context = {"execution_date": "{{ execution_date }}"}


with dag:

    # Wait for agg1d
    ETS_agg1d = ExternalTaskSensor(external_dag_id="email_aggs1d",
                                   external_task_id="agg1d_stop",
                                   task_id="wait_agg1d",
                                   timeout=72000)

    # -------------- WORK -----------------

    f = extract_many_email_aggs1d
    PO_extract_email_aggs = PythonOperator(task_id=f.__name__, python_callable=f,
                                           provide_context=True, op_kwargs={"days": 90})

    f = generate_email_aggsnd
    PO_generate_email_aggs90d = PythonOperator(task_id=f.__name__, python_callable=f,
                                               provide_context=True, op_kwargs={"days": 90})

    f = extract_email_aggs
    PO_extract_email_aggs_2 = PythonOperator(task_id=f.__name__, python_callable=f,
                                             provide_context=True, op_kwargs={"agg_number": 90})

    f = generate_email_aggsnd_ci
    PO_generate_email_aggs90d_ci = PythonOperator(task_id=f.__name__, python_callable=f,
                                                  provide_context=True, op_kwargs={"days": 90})

    f = generate_email_aggsnd_report
    PO_generate_email_aggs90d_report = PythonOperator(task_id=f.__name__, python_callable=f,
                                                      provide_context=True, op_kwargs={"days": 90})

    DO_agg90d_stop = DummyOperator(task_id="agg90dci_stop", trigger_rule="one_success")

    # ----------- ALL DEPENDENCIES ---------------

    ETS_agg1d >> PO_extract_email_aggs >> PO_generate_email_aggs90d >> PO_extract_email_aggs_2 >> \
        PO_generate_email_aggs90d_ci >> PO_generate_email_aggs90d_report >> DO_agg90d_stop

