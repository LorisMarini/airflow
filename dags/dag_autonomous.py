from skepsi.smart_content.v1 import v1_rank_contents
from skepsi.smart_content.v1 import v1_output_dashboard
from skepsi.smart_content.v1 import v1_clean_temp
from skepsi.smart_content.v1 import v1_output_cache

from skepsi.smart_delay.v1 import v1_parallel_process_smart_delay
from skepsi.smart_delay.v1 import v1_output_cache_smart_delay
from skepsi.smart_delay.v1 import v1_clean_smart_delay_temps

from skepsi.smart_interval.smart_interval import parallel_smart_interval
from skepsi.smart_interval.smart_interval import build_smart_interval_output
from skepsi.smart_interval.smart_interval import clean_temp_smart_interval
from skepsi.smart_interval.smart_interval import build_smart_interval_dashboard_output

from skepsi.utils.etl import task_fail_slack_alert
from skepsi.utils.imports import *


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
dag = DAG('autonomous', catchup=True, max_active_runs=1, default_args=default_args, schedule_interval='@daily')

# Retrieve execution date from template
# This will be a datetime only during a DAG run
context = {"execution_date": "{{ execution_date }}"}


# ------------------ SMART CONTENT ------------------
# ------------------ SMART CONTENT ------------------
# ------------------ SMART CONTENT ------------------

with dag:

    # -------------- AGG SENSOR -----------------

    # Wait for email results to be available
    ETS_email_anomaly_ci = ExternalTaskSensor(external_dag_id="email_outliers_ci",
                                              external_task_id="clean_temp_email_cross_instance",
                                              task_id="wait_email_outliers_ci",
                                              timeout=72000)

    # ----------------- WORK --------------------

    # Generate insights
    f = v1_rank_contents
    PO_rank_contents = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    # Load results to storage
    f = v1_output_cache
    PO_archive_cache = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    # Load results to storage
    f = v1_output_dashboard
    PO_output_dashboard = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    # Clean local copies of assets
    f = v1_clean_temp
    PO_clean_temp = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    # ----------- DEPENDENCIES ---------------

    ETS_email_anomaly_ci >> PO_rank_contents >> PO_archive_cache >> PO_output_dashboard >> PO_clean_temp


# ------------------ SMART DELAY ------------------
# ------------------ SMART DELAY ------------------
# ------------------ SMART DELAY ------------------

with dag:

    # -------------- TASK SENSOR -----------------
    # Wait for records to be available
    SD_ETS_records = ExternalTaskSensor(external_dag_id="records",
                                        external_task_id="records_stop",
                                        task_id="wait_records",
                                        timeout=72000)

    # --------------- LOGIC  ----------------

    f = v1_parallel_process_smart_delay
    PO_parallel_extract_smart_delay = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    f = v1_output_cache_smart_delay
    PO_output_cache_smart_delay = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    f = v1_clean_smart_delay_temps
    PO_clean_smart_delay_temp = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    # -------------- DEPENDENCIES -----------------

    SD_ETS_records >> PO_parallel_extract_smart_delay >> PO_output_cache_smart_delay >> PO_clean_smart_delay_temp

# ------------------ SMART INTERVAL ------------------
# ------------------ SMART INTERVAL ------------------
# ------------------ SMART INTERVAL ------------------

with dag:

    # -------------- AGG SENSOR -----------------

    # Wait for records to be available
    ETS_records = ExternalTaskSensor(external_dag_id="records",
                                     external_task_id="records_stop",
                                     task_id="wait_records",
                                     timeout=72000)

    # ----------------- WORK --------------------

    # Generate insights
    f = parallel_smart_interval
    PO_parallel_smart_interval = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    # Load results to storage
    f = build_smart_interval_output
    PO_output = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    f = build_smart_interval_dashboard_output
    PO_dashboard_output = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    # Clean local copies of assets
    f = clean_temp_smart_interval
    PO_clean_temp = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    # ----------- DEPENDENCIES ---------------

    ETS_records >> PO_parallel_smart_interval >> PO_output >> PO_dashboard_output >> PO_clean_temp
