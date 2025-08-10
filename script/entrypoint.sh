#!/bin/bash
set -e

# Step 1: Install requirements if present
if [ -e "/opt/airflow/requirements.txt" ]; then
  python -m pip install --upgrade pip
  pip install --user -r /opt/airflow/requirements.txt
fi

# Step 2: Ensure logs directory exists and has correct permissions
mkdir -p /opt/airflow/logs/scheduler
chmod -R 777 /opt/airflow/logs

# Step 3: Initialize Airflow database and create user if not already done
if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init && \
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin
fi

# Step 4: Upgrade DB and launch webserver
airflow db upgrade

exec airflow webserver




# set -e

# if [ -e "/opt/airflow/requirements.txt" ]; then
#   $(command python) pip install --upgrade pip
#   $(command -v pip) install --user -r requirements.txt
# fi

# if [ ! -f "/opt/airflow/airflow.db" ]; then
#   airflow db init && \
#   airflow users create \
#     --username admin \
#     --firstname admin \
#     --lastname admin \
#     --role Admin \
#     --email admin@example.com \
#     --password admin
# fi

# $(command -v airflow) db upgrade

# exec airflow webserver