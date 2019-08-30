# TODO SUSPENDED DAG !!! DO NOT DELETE.

# TODO Before re-activating it, make sure that the following etlids are not needed anymore:
# TODO generated the object:

# TODO 1. etlid_activity_record()
# TODO 2. etlid_engagement_record()
# TODO 3. etlid_sent_record()
# TODO 4. etlid_interaction_record()

# from skepsi.records.engagement import derived_records
# from skepsi.records.activities_all import extract_activities_90d
# from skepsi.records.activities_all import activity_records
#
# from skepsi.utils.etl import task_fail_slack_alert
# from skepsi.utils.utils import *
#
# # ------------------ INSTANTIATE DAG ------------------
#
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2019, 4, 10),
#     'email': ['loris@autopilothq.com'],
#     'email_on_failure': True,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(seconds=3),
#     'on_failure_callback': task_fail_slack_alert
# }
#
# # Dag Declaration
# dag = DAG('records', catchup=True, max_active_runs=1, default_args=default_args, schedule_interval='@daily')
#
# # Retrieve execution date from template
# # This will be a datetime only during a DAG run
# context = {"execution_date": "{{ execution_date }}"}
#
#
# with dag:
#
#     # -------------- DUMMY -----------------
#
#     D_records_stop = DummyOperator(task_id="records_stop")
#
#     # Wait for record DAG
#     ETS_activity_records = ExternalTaskSensor(external_dag_id="dataprep",
#                                               external_task_id="clean_temps",
#                                               task_id="wait_dataprep",
#                                               timeout=72000)
#
#     # Wait for record DAG
#     ETS_subscriptions = ExternalTaskSensor(external_dag_id="subscriptions",
#                                            external_task_id="sub_stop",
#                                            task_id="wait_subs",
#                                            timeout=72000)
#
#     # -------------- WORK -----------------
#
#     f = extract_activities_90d
#     PO_extract_activities_90d = PythonOperator(task_id=f.__name__, python_callable=f,
#                                                provide_context=True, op_kwargs={"use_cache": True})
#
#     f = activity_records
#     PO_activity_records = PythonOperator(task_id=f.__name__, python_callable=f,
#                                          provide_context=True, op_kwargs={"use_cache":True})
#
#     f = derived_records
#     PO_derived_records = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)
#
#     # -------------- DEPENDENCIES -----------------
#
#     [ETS_activity_records, ETS_subscriptions] >> PO_extract_activities_90d >> \
#         PO_activity_records >> PO_derived_records >> D_records_stop
