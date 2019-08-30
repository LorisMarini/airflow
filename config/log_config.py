import os
from airflow import configuration as conf

# Get the env variable for the remote my_logger
GCS_LOG_FOLDER = os.environ.get('AIRFLOW__CORE__REMOTE_BASE_LOG_FOLDER')

LOG_LEVEL = conf.get('core', 'LOGGING_LEVEL').upper()
LOG_FORMAT = conf.get('core', 'log_format')

BASE_LOG_FOLDER = conf.get('core', 'BASE_LOG_FOLDER')
PROCESSOR_LOG_FOLDER = conf.get('scheduler', 'child_process_log_directory')

# FILENAME_TEMPLATE = '{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log'
FILENAME_TEMPLATE = '{{ ti.dag_id }}_{{ ts }}.log'


PROCESSOR_FILENAME_TEMPLATE = '{{ filename }}.log'

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'airflow.task': {
            'format': LOG_FORMAT,
        },
        'airflow.processor': {
            'format': LOG_FORMAT,
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'airflow.task',
            'stream': 'ext://sys.stdout'
        },
        'file.task': {
            'class': 'airflow.utils.log.file_task_handler.FileTaskHandler',
            'formatter': 'airflow.task',
            'base_log_folder': os.path.expanduser(BASE_LOG_FOLDER),
            'filename_template': FILENAME_TEMPLATE,
        },
        'file.processor': {
            'class': 'airflow.utils.log.file_processor_handler.FileProcessorHandler',
            'formatter': 'airflow.processor',
            'base_log_folder': os.path.expanduser(PROCESSOR_LOG_FOLDER),
            'filename_template': PROCESSOR_FILENAME_TEMPLATE,
        },
        'gcs.task': {
            'class': 'airflow.utils.log.gcs_task_handler.GCSTaskHandler',
            'formatter': 'airflow.task',
            'base_log_folder': os.path.expanduser(BASE_LOG_FOLDER),
            'gcs_log_folder': GCS_LOG_FOLDER,
            'filename_template': FILENAME_TEMPLATE,
        },
    },
    'loggers': {
        '': {
            'handlers': ['console'],
            'level': LOG_LEVEL,
        },
        'airflow': {
            'handlers': ['console'],
            'level': LOG_LEVEL,
            'propagate': False,
        },
        'airflow.processor': {
            'handlers': ['file.processor'],
            'level': LOG_LEVEL,
            'propagate': True,
        },
        'airflow.task': {
            'handlers': ['gcs.task'],
            'level': LOG_LEVEL,
            'propagate': False,
        },
        'airflow.task_runner': {
            'handlers': ['gcs.task'],
            'level': LOG_LEVEL,
            'propagate': True,
        },
    }
}
