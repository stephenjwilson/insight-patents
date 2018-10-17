from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os

airflow_args = {
    'owner': 'swilson',
    'depends_on_past': False,
    'retries': 0,
    'start_date': datetime(2018, 9, 1),
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('airflow_weekly', default_args=airflow_args, schedule_interval='0 14 * * TUE')
now = datetime.now()

download_data = BashOperator(task_id='download_data',
                             bash_command='python3 /home/ubuntu/insight-patents/src/python/download.py {0} {0} True'.format(now.strftime('%Y$m%d')),
                             dag=dag)

process_data = BashOperator(task_id='process_data',
                            bash_command='PYTHONPATH=/home/ubuntu/insight-patents/dist/insight_patents-0.0.0-py3.5.egg python3 /home/ubuntu/insight-patents/src/python/parse_patents.py --bulk=false --date {}'.format(now.strftime('%Y$m%d')),
                            dag=dag)  # neo4j specific upload
process_data.set_upstream(download_data)
