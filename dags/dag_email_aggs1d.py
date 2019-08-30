from skepsi.emails.agg import parallel_email_aggs1d_from_pq
from skepsi.emails.agg import load_email_aggs
from skepsi.emails.agg import clean_email_aggs1d
from skepsi.dataprep.activities_pq import act_pq_extract

from skepsi.utils.etl import task_fail_slack_alert
from skepsi.utils.imports import *


# ------------------ INSTANTIATE DAG ------------------

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 6),
    'email': ['loris@autopilothq.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=3),
    'on_failure_callback': task_fail_slack_alert
}

# Dag Declaration
dag = DAG('email_aggs1d', catchup=True, max_active_runs=1, default_args=default_args, schedule_interval='@daily')

# Retrieve execution date from template
# This will be a datetime only during a DAG run
context = {"execution_date": "{{ execution_date }}"}


with dag:  # aggs1d
    # -------------- DUMMY -----------------

    agg1d_stop = DummyOperator(task_id="agg1d_stop", trigger_rule="one_success")

    # -------------- WAIT FOR DATAPREP -----------------

    # Wait for dataprep
    ETS_dataprep = ExternalTaskSensor(external_dag_id="dataprep",
                                      external_task_id="clean_temps",
                                      task_id="wait_dataprep",
                                      timeout=72000)

    # -------------- WORK -----------------

    # Extract
    f = act_pq_extract
    TO_extract_activities_pq_tar = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True,
                                                  op_kwargs={"missing_src_ok":  False})

    # Transform
    f = parallel_email_aggs1d_from_pq
    TO_parallel_email_aggs1d_from_pq = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    # Load
    f = load_email_aggs
    TO_load_email_aggs_1d = PythonOperator(task_id=f.__name__, python_callable=f,
                                           provide_context=True, op_kwargs={"agg_number": 1})

    f = clean_email_aggs1d
    TO_clean_email_aggs1d = PythonOperator(task_id=f.__name__, python_callable=f,
                                           provide_context=True, op_kwargs={"agg_number": 1})

    # ----------- ALL DEPENDENCIES ---------------

    ETS_dataprep >> TO_extract_activities_pq_tar >> TO_parallel_email_aggs1d_from_pq >> \
        TO_load_email_aggs_1d >> TO_clean_email_aggs1d >> agg1d_stop

