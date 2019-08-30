from skepsi.utils.etl import task_fail_slack_alert
from skepsi.utils.utils import *

from skepsi.customers.utils import radar_dataset_ranked
from skepsi.customers.billing import billed_contacts_all
from skepsi.customers.identity import customers_identity
from skepsi.customers.engagement import customer_engagement

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
dag = DAG('customers', catchup=True, max_active_runs=1, default_args=default_args, schedule_interval='@daily')

# Retrieve execution date from template
# This will be a datetime only during a DAG run
context = {"execution_date": "{{ execution_date }}"}


with dag:

    # Wait for subscriptions DAG
    ETS_subscriptions = ExternalTaskSensor(external_dag_id="subscriptions",
                                           external_task_id="sub_stop",
                                           task_id="wait_subscriptions",
                                           timeout=72000)

    f = billed_contacts_all
    PO_billed_contacts_all = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    f = customers_identity
    PO_customers_identity = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    f = customer_engagement
    PO_customer_engagement = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    f = radar_dataset_ranked
    PO_raw_data_for_modelling = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    # -------------- DEPENDENCIES -----------------

    ETS_subscriptions >> PO_billed_contacts_all >> \
        PO_customers_identity >> PO_customer_engagement
