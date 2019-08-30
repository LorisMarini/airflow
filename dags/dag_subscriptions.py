from skepsi.customers.subscriptions import subs_query
from skepsi.customers.subscriptions import subs_tz_locations

from skepsi.utils.imports import *
from skepsi.utils.etl import task_fail_slack_alert
from skepsi.utils.etl import post_message_to_slack

# ------------------ INSTANTIATE DAG ------------------

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 8, 1),
    'email': ['loris@autopilothq.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=3),
    'on_failure_callback': task_fail_slack_alert
}

# Dag Declaration
dag = DAG('subscriptions', catchup=True,
          default_args=default_args, schedule_interval='@daily')

with dag:

    # --------------- NOTIFICATIONS  ----------------

    payload = {"message": ":circleci-pass: Data pipeline started processing.",
               "channel_type": "ALERTS",
               "text_type": "markdown"}

    f = post_message_to_slack
    SO_start_slack = PythonOperator(task_id=f.__name__, python_callable=f, trigger_rule="all_success",
                                    op_kwargs=payload, provide_context=True)

    # --------------- WORK  ----------------

    f = subs_query
    PO_susb_query = PythonOperator(
        task_id=f.__name__, python_callable=f, provide_context=True)

    f = subs_tz_locations
    PO_subs_tz = PythonOperator(
        task_id=f.__name__, python_callable=f, provide_context=True)

    DO_sub_stop = DummyOperator(task_id="sub_stop", trigger_rule="one_success")

    # --------------- DEPENDENCIES  ----------------

    SO_start_slack >> PO_susb_query >> PO_subs_tz >> DO_sub_stop
