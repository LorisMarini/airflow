"""
Description Here
"""

"""
Import statements HERE
"""

# ------------------ INSTANTIATE DAG ------------------

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 12, 19),
    'email': ['lorenzo.marini.au@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=3),
    'on_failure_callback': task_fail_slack_alert
}

# Dag Declaration
dag = DAG('dataprep', catchup=True, max_active_runs=1, default_args=default_args, schedule_interval='@daily')

# Retrieve execution date from template
# This will be a datetime only during a DAG run
context = {"execution_date": "{{ execution_date }}"}


with dag:

    # Wait for record DAG
    ETS_subscriptions = ExternalTaskSensor(external_dag_id="subscriptions",
                                           external_task_id="sub_stop",
                                           task_id="wait_subs",
                                           timeout=72000)

    DO_raw_skip_1 = DummyOperator(task_id='raw_skip_1')
    DO_raw_skip_2 = DummyOperator(task_id='raw_skip_2')
    DO_raw_skip_3 = DummyOperator(task_id='raw_skip_3')
    DO_raw_stop_1 = DummyOperator(task_id="raw_stop_1", trigger_rule="one_success")

    # ------------------ BRANCH ------------------

    def jsons_in_tmp(**context):
        raw_dir = etlid_activity_raw("test", **context).temp_dir
        if path_is_empty(raw_dir):
            return TO_extract_act.task_id
        else:
            return DO_raw_skip_3.task_id

    def tar_in_tmp(**context):
        tar_path = etlid_activities_raw_tar(**context).temp_path
        if path_exists(tar_path):
            return DO_raw_skip_2.task_id
        else:
            return TO_json_in_tmp.task_id

    def tar_in_bucket(**context):
        tar_path = etlid_activities_raw_tar(**context).prod_path
        if path_exists(tar_path):
            return DO_raw_skip_1.task_id
        else:
            return TO_tar_in_tmp.task_id

    f = jsons_in_tmp
    TO_json_in_tmp = BranchPythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    f = tar_in_tmp
    TO_tar_in_tmp = BranchPythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    f = tar_in_bucket
    TO_tar_in_bucket = BranchPythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    # -------------- WORK -----------------

    f = act_raw_query_all
    TO_extract_act = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    f = act_raw_archive
    TO_archive = PythonOperator(task_id=f.__name__, python_callable=f,
                                provide_context=True, trigger_rule="one_success")

    f = act_raw_load
    TO_load_raw_tar = PythonOperator(task_id=f.__name__, python_callable=f,
                                     provide_context=True, trigger_rule="one_success")

    # -------------  GRAPH  -------------------

    ETS_subscriptions >> TO_tar_in_bucket

    TO_tar_in_bucket >> TO_tar_in_tmp
    TO_tar_in_bucket >> DO_raw_skip_1 >> DO_raw_stop_1

    TO_tar_in_tmp >> DO_raw_skip_2 >> TO_load_raw_tar
    TO_tar_in_tmp >> TO_json_in_tmp

    TO_json_in_tmp >> DO_raw_skip_3 >> TO_archive
    TO_json_in_tmp >> TO_extract_act

    TO_extract_act >> TO_archive >> TO_load_raw_tar >> DO_raw_stop_1


with dag:  # Parquet files

    # ------------------ BRANCH ------------------

    def pq_in_bucket(**context):
        tar_path = etlid_activity_pq_tar(**context).prod_path
        if path_exists(tar_path):
            return DO_pq_skip.task_id
        else:
            return PO_extract_activities_raw.task_id

    f = pq_in_bucket
    BO_pq_in_bucket = BranchPythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    # ----------------- WORK ----------------------

    DO_pq_skip = DummyOperator(task_id='pq_skip')
    DO_pq_done = DummyOperator(task_id='pq_done', trigger_rule="one_success")

    f = act_raw_extract_expand
    PO_extract_activities_raw = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    f = act_pq_from_json_all
    PO_parallel_json2engpq = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    f = act_pq_archive_load
    PO_activities_archive_load = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    f = clean_temps
    PO_clean_temps = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    # --------------------- SLACK POST --------------------------

    f = post_message_to_slack
    message = ":circleci-pass: Data pipeline completed processing."
    slack_post_kwargs = {"message": message,
                         "channel_type": "ALERTS",
                         "text_type": "markdown"}

    SO_end_slack = PythonOperator(task_id=f.__name__, python_callable=f, trigger_rule="all_done",
                                  op_kwargs=slack_post_kwargs, provide_context=True)

    # -------------  GRAPH  -------------------

    DO_raw_stop_1 >> BO_pq_in_bucket >> DO_pq_skip >> DO_pq_done
    BO_pq_in_bucket >> PO_extract_activities_raw >> PO_parallel_json2engpq >> PO_activities_archive_load
    PO_activities_archive_load >> PO_clean_temps >> DO_pq_done >> SO_end_slack
