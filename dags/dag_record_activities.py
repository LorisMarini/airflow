"""
This DAG is designed to run downstream of `gbq_activities` dag. It
    1.

!!! WARNING !!!
The first part of this dag uses GCS as the cache layer for BigQuery. This is
done to avoid re-running the queries when backfiling the DAG and thus control
costs. If activity_count_day or gbq_ingest_activity_type need to re-ingest data into
BigQuery set the kwarg `use_cache` to False and take due precautions to remove duplicates
in tables.
"""

from skepsi.records.activities import activity_count_day
from skepsi.records.activities import activity_count_weekly

from skepsi.utils.etl import task_fail_slack_alert
from skepsi.utils.utils import *

# ------------------ INSTANTIATE DAG ------------------

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 4, 1),
    'email': ['loris@autopilothq.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=3),
    'on_failure_callback': task_fail_slack_alert
}

# Dag Declaration
dag = DAG('gbq_activities_derived', catchup=True, max_active_runs=10,
          default_args=default_args, schedule_interval='@daily')

# Retrieve execution date from template
# This will be a datetime only during a DAG run
context = {"execution_date": "{{ execution_date }}"}

with dag:

    # -------------- DUMMY -----------------

    # Wait for record DAG
    ETS_gbq_activities = ExternalTaskSensor(external_dag_id="gbq_activities",
                                            external_task_id="gbq_done",
                                            task_id="wait_gbq_daily_ingestion",
                                            timeout=72000)

    f = activity_count_day
    PO_activity_count_day = PythonOperator(task_id=f.__name__, python_callable=f,
                                           provide_context=True, op_kwargs={"use_cache": True})

    f = activity_count_weekly
    PO_activity_count_weekly = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    DO_stop = DummyOperator(task_id="stop")

    # -------------- DEPENDENCIES -----------------

    ETS_gbq_activities >> \
        PO_activity_count_day >> PO_activity_count_weekly >> DO_stop
