from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models import Variable

import pendulum
import json
import logging

"""
This file demonstrates a simple Airflow DAG that triggers a the Airbyte API running in Airbyte Cloud
The operations that are demonstrated are "sync" and "jobs" (status).
"""

AIRBYTE_CONNECTION_ID = Variable.get("MY_EXAMPLE_CONNECTION_ID")
API_KEY = f'Bearer {Variable.get("CLOUD_API_TOKEN")}'

task_logger = logging.getLogger('airflow.task')

with DAG(dag_id='airbyte_api_sync_demo',
         default_args={'owner': 'airflow'},
         schedule='@daily',
         start_date=pendulum.today('UTC').add(days=-1)
         ) as dag:

    trigger_sync = SimpleHttpOperator(
        method="POST",
        task_id='start_airbyte_sync',
        http_conn_id='airbyte-api-cloud-connection',
        headers={
            "Content-Type":"application/json",
            "User-Agent": "fake-useragent",  # Airbyte cloud requires that a user agent is defined
            "Accept": "application/json",
            "Authorization": API_KEY},
        endpoint=f'/v1/connections/{AIRBYTE_CONNECTION_ID}/sync',
        do_xcom_push=True,
        response_filter=lambda response: response.json()['jobId'],
        log_response=True,
    )

    wait_for_sync_to_complete = HttpSensor(
        method='GET',
        task_id='wait_for_airbyte_sync',
        http_conn_id='airbyte-api-cloud-connection',
        headers={
            "Content-Type":"application/json",
            "User-Agent": "fake-useragent",  # Airbyte cloud requires that a user agent is defined
            "Accept": "application/json",
            "Authorization": API_KEY},
        endpoint='/v1/jobs/{}'.format("{{ task_instance.xcom_pull(task_ids='start_airbyte_sync') }}"),
        poke_interval=5,
        response_check=lambda response: json.loads(response.text)['status'] == "succeeded"
    )

    trigger_sync >> wait_for_sync_to_complete

    if __name__ == "__main__":
        dag.test()