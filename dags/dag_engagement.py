"""
TODO - SUSPENDED UNTIL record_interactions() is re-written to use BigQuery.
"""

from skepsi.engagement.engagement import parallel_engagement_metrics

from skepsi.utils.etl import task_fail_slack_alert
from skepsi.utils.utils import *


# ------------------ INSTANTIATE DAG ------------------

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 4, 10),
    'email': ['loris@autopilothq.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=3),
    'on_failure_callback': task_fail_slack_alert
}


# Dag Declaration
dag = DAG('engagement', catchup=True, default_args=default_args, schedule_interval='@daily')

# Retrieve execution date from template
# This will be a datetime only during a DAG run
context = {"execution_date": "{{ execution_date }}"}


with dag:

    D_engagement_stop = DummyOperator(task_id="engagement_stop")

    # Wait for record DAG
    ETS_engagement_records = ExternalTaskSensor(external_dag_id="records",
                                                external_task_id="records_stop",
                                                task_id="wait_for_engagement_records",
                                                timeout=72000)

    f = parallel_engagement_metrics
    PO_engagement_metrics = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    # -------------- DEPENDENCIES -----------------

    ETS_engagement_records >> PO_engagement_metrics >> D_engagement_stop

