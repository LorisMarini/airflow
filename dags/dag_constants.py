"""
This DAG is designed to perform non-periodic operations, necessary to load
tables or asset to and from bucket and warehouse.
"""
from skepsi.records.activities import gbq_ingest_activity_type

from skepsi.utils.etl import task_fail_slack_alert
from skepsi.utils.utils import *

# ------------------ INSTANTIATE DAG ------------------

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 4, 15),
    'email': ['loris@autopilothq.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=3),
    'on_failure_callback': task_fail_slack_alert,
    'max_active_runs': 1,
    'schedule_interval': '@once'
}

# Dag Declaration
dag = DAG('constants', default_args=default_args)

# Retrieve execution date from template
# This will be a datetime only during a DAG run
context = {"execution_date": "{{ execution_date }}"}

with dag:

    DO_start = DummyOperator(task_id="start")

    f = gbq_ingest_activity_type
    PO_gbq_ingest_activity_type = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    # ------------- DEPENDENCIES --------------

    DO_start >> PO_gbq_ingest_activity_type

