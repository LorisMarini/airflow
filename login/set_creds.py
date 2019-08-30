#!/opt/conda/bin/python
import airflow
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser
import os
"""
The following is adapted from https://airflow.readthedocs.io/en/stable/security.html
"""
if __name__ == "__main__":

    # Extract new username and password
    username = os.environ.get("AIRFLOW_INITIAL_USER_USERNAME", None)
    password = os.environ.get("AIRFLOW_INITIAL_USER_PASSWORD", None)

    if not username:
        raise ValueError(f"env variable AIRFLOW_INITIAL_USER_USERNAME must be difined")

    if not password:
        raise ValueError(f"env variable AIRFLOW_INITIAL_USER_PASSWORD must be difined")

    # Open a session to change the settings
    session = settings.Session()

    # Delete existing users
    session.query(models.User).delete()

    # Get a User object
    user = PasswordUser(models.User())

    # Try to make changes to it
    try:
        user.username = username
    except Exception as ex:
        print(f"Could not set user. Error returned: {str(ex)}")
    try:
        user.password = password
    except Exception as ex:
        print(f"Could not set password. Error returned: {str(ex)}")
    try:
        user.email = 'loris@autopilohq.com'
    except Exception as ex:
        print(f"Could not set email. Error returned: {str(ex)}")

    # Add new user settings
    session.add(user)

    # Commit settings and close session
    session.commit()
    session.close()

    # Terminate script
    exit()
