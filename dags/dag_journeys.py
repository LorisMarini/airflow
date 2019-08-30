from skepsi.utils.utils import *
from skepsi.utils.etl import task_fail_slack_alert

from skepsi.journey.index import jidx_from_cloudant_all
from skepsi.journey.index import jidx_gbq_load

from skepsi.journey.canvas import canvas_info_all
from skepsi.journey.canvas import canvas_gbq_load_states
from skepsi.journey.canvas import canvas_gbq_load_shapes


# ------------------ INSTANTIATE DAG ------------------

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 1),
    'email': ['loris@autopilothq.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=3),
    'on_failure_callback': task_fail_slack_alert
}

# Dag Declaration
dag = DAG('journey', catchup=False, default_args=default_args, schedule_interval='@daily', max_active_runs=1)

# Retrieve execution date from template
# This will be a datetime only during a DAG run
context = {"execution_date": "{{ execution_date }}"}

with dag:

    DO_stop = DummyOperator(task_id="journey_stop", trigger_rule="all_success")

    # Wait for subscriptions DAG
    ETS_subscriptions = ExternalTaskSensor(external_dag_id="subscriptions",
                                           external_task_id="sub_stop",
                                           task_id="wait_subscriptions",
                                           timeout=72000)

    f = jidx_from_cloudant_all
    PO_journey_index_all = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True,
                                          op_kwargs={"cache": True})

    f = jidx_gbq_load
    PO_jidx_gbq_load = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    f = canvas_info_all
    PO_canvas_info_all = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True,
                                        op_kwargs={"cache": True})

    f = canvas_gbq_load_states
    PO_load_states = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    f = canvas_gbq_load_shapes
    PO_load_shapes = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    # -------------- DEPENDENCIES -----------------

    ETS_subscriptions >> PO_journey_index_all >> PO_jidx_gbq_load >> \
        PO_canvas_info_all >> PO_load_states >> PO_load_shapes >> DO_stop
