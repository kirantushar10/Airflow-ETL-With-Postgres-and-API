from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator ## USES TO CALL API 
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook   ## USES TO PUSH DATA TO POSTGRES
from datetime import datetime, timedelta
import pendulum
import json

## DEFINE DAG

with DAG(
    dag_id='nasa_apod_postgres_etl',
    start_date = pendulum.now("UTC").subtract(days=1),
    schedule='@daily',
    catchup=False,
) as dag:

    ## TASK 1: CREATE TABLE IF NOT EXISTS
    @task
    def create_table():
        ## INTIALIZE POSTGRES HOOK - INTERFACE WITH POSTGRES
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_connection')

        ## DEFINE CREATE TABLE SQL
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS nasa_apod (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """
        ## EXECUTE CREATE TABLE SQL
        pg_hook.run(create_table_sql)

    ## TASK 2: FETCH DATA FROM NASA APOD API - EXTRACT PIPELINE
    extract_apod = HttpOperator(
        task_id='extract_apod',
        method='GET',
        http_conn_id='nasa_api',   ## CONNECTION ID FROM AIRFLOW CONNECTIONS
        endpoint='planetary/apod',  ## NASA API ENDPOINT
        data={'api_key': '{{conn.nasa_api.extra_dejson.api_key}}'},  ## USE THE API KEY FROM CONNECTION
        response_filter=lambda response:response.json(), ## CONVERT RESPONSE TO JSON
    )



    ## TASK 3: TRANSFORM DATA
    @task
    def transform_data(response):
        transform_data = {
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', ''),
        }
        return transform_data

    ## TASK 4: LOAD DATA INTO POSTGRES
    @task
    def load_data_to_postgres(data):
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_connection')

        insert_sql = """
        INSERT INTO nasa_apod (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """
        pg_hook.run(insert_sql, parameters=(
            data['title'],
            data['explanation'],
            data['url'],
            data['date'],
            data['media_type']
        ))


    ## TASK 5: VERIFY DATA IN POSTGRES USING DBVIEWER
    


    ## DEFINE TASK DEPENDENCIES
    ## EXTRACT
    create_table() >> extract_apod
    api_response = extract_apod.output
    ## TRANSFORM
    transformed_data = transform_data(api_response)
    ## LOAD
    load_data_to_postgres(transformed_data)
