"""
This DAG is designed to run downstream of `dataprep`. It
1. extracts daily parquet data on disk (one per instance)
2. combines them into chunks of nearly similar size and loads them to GCS
3. schedules load-jobs from GCS to BigQuery table `activities` partitioned by timestamp
"""

from skepsi.utils.etl import task_fail_slack_alert

from skepsi.dataprep.activities_pq import act_pq_extract
from skepsi.dataprep.activities_gbq import act_gbq_ingest_all
from skepsi.dataprep.activities_gbq import act_gbq_prepare_ingestion
from skepsi.dataprep.activities_gbq import act_gbq_clean_temps

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
    'on_failure_callback': task_fail_slack_alert
}

# Dag Declaration
dag = DAG('gbq_activities', catchup=True, max_active_runs=10, default_args=default_args, schedule_interval='@daily')

# Retrieve execution date from template
# This will be a datetime only during a DAG run
context = {"execution_date": "{{ execution_date }}"}


with dag:

    # Wait for dataprep
    ETS_dataprep = ExternalTaskSensor(external_dag_id="dataprep",
                                      external_task_id="clean_temps",
                                      task_id="wait_dataprep",
                                      timeout=72000)

    f = act_pq_extract
    TO_act_pq_extract = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True,
                                       op_kwargs={"missing_src_ok": False})

    f = act_gbq_prepare_ingestion
    PO_prepare_activities_for_gbq = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True,
                                                   trigger_rule="all_success")

    f = act_gbq_ingest_all
    PO_ingest_activities_in_gbq = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True,
                                                 trigger_rule="all_success")

    f = act_gbq_clean_temps
    PO_act_gbq_clean_temps = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True,
                                            trigger_rule="all_done")

    DO_gbq_done = DummyOperator(task_id='gbq_done', trigger_rule="one_success")

    # -------------  GRAPH  -------------------

    ETS_dataprep >> TO_act_pq_extract >> \
        PO_prepare_activities_for_gbq >> PO_ingest_activities_in_gbq >> \
        PO_act_gbq_clean_temps >> DO_gbq_done
