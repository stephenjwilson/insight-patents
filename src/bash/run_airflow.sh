#!/usr/bin/env bash

# Run on master

# Register the dag
python3 ~/insight-patents/src/airflow/dags/weekly_update.py

# Init airflow
airflow webserver -p 8081 &
airflow scheduler &