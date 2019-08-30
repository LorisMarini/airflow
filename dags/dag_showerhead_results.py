from skepsi.showerhead.make_results import sh_results_email_anomaly
from skepsi.showerhead.make_results import sh_results_goal_performance
from skepsi.utils.etl import task_fail_slack_alert
from skepsi.utils.caching import reset_redis_cache
from skepsi.showerhead.make_results import post_reports_to_slack
from skepsi.utils.etl import post_message_to_slack

from skepsi.utils.imports import *

# ------------------ INSTANTIATE DAG ------------------

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 2, 13),
    'email': ['loris@autopilothq.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=3),
    'on_failure_callback': task_fail_slack_alert
}

# Dag Declaration
dag = DAG('showerhead_results', catchup=True, max_active_runs=3,
          default_args=default_args, schedule_interval='@daily')

# Retrieve execution date from template
# This will be a datetime only during a DAG run
context = {"execution_date": "{{ execution_date }}"}


with dag:

    # -------------- DAG dataprep SENSOR -----------------

    # Wait for email results to be available
    ETS_email = ExternalTaskSensor(external_dag_id="email_outliers",
                                   external_task_id="email_results_stop",
                                   task_id="wait_email_outliers",
                                   timeout=72000)

    # Wait for email results to be available
    ETS_email_ci = ExternalTaskSensor(external_dag_id="email_outliers_ci",
                                      external_task_id="clean_temp_email_cross_instance",
                                      task_id="wait_email_outliers_ci",
                                      timeout=72000)

    # Wait for goal results to be available
    ETS_goals = ExternalTaskSensor(external_dag_id="goal_performance",
                                   external_task_id="clean_goal_temps",
                                   task_id="wait_goal_performance",
                                   timeout=72000)

    # --------------------- SLACK POST --------------------------

    f = post_message_to_slack
    message = ":circleci-pass: Data pipeline completed processing."
    slack_post_kwargs = {"message": message,
                         "channel_type": "ALERTS",
                         "text_type": "markdown"}

    SO_end_slack = PythonOperator(task_id=f.__name__, python_callable=f, trigger_rule="all_done",
                                  op_kwargs=slack_post_kwargs, provide_context=True)

    # --------------------- WORK --------------------------

    f = sh_results_email_anomaly
    PO_results_emails = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True,
                                       trigger_rule="all_success")

    f = sh_results_goal_performance
    PO_results_goals = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True)

    # Reset cache for email anomaly
    f = reset_redis_cache
    rce_id = f.__name__ + "_" + "emails"
    PO_reset_cache_email = PythonOperator(task_id=rce_id, python_callable=f, provide_context=True,
                                          op_kwargs={"insight": "email-anomaly"})

    # Reset cache for goal performance
    f = reset_redis_cache
    rcg_id = f.__name__ + "_" + "goals"
    PO_reset_cache_goals = PythonOperator(task_id=rcg_id, python_callable=f, provide_context=True,
                                          op_kwargs={"insight": "goal-performance"})

    # --------------------  POST REPORTS TO SLACK ------------------------

    f = post_reports_to_slack
    PO_slack_reports = PythonOperator(task_id=f.__name__, python_callable=f, provide_context=True,
                                      trigger_rule="all_success")

    # -------------- DEPENDENCIES -----------------

    [ETS_email, ETS_email_ci] >> PO_results_emails >> PO_reset_cache_email >> PO_slack_reports

    ETS_goals >> PO_results_goals >> PO_reset_cache_goals >> PO_slack_reports

    PO_slack_reports >> SO_end_slack
