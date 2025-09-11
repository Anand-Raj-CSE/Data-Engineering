from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json

# Definfing DAG
with DAG(
    dag_id = 'Nasa_apod_postgres',
    start_date = days_ago(1),
    schedule = '@daily',
    catchup = False
)as dag:
    # Step 1 : Create table if it does not exists
    @task
    def create_table():
        # initialize postgres hook
        postgres_hook = PostgresHook(postgres_conn_id = "my_postgres_connection")

        # SQL query to create the table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explaination TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50) 
        );
        """
        # Execute this query 
        postgres_hook.run(create_table_query)

    # Step 2 : Extract the NASA API Data APOD - Astronomy Picture of the Day [Extract pipeline]
    #  https://api.nasa.gov/planetary/apod?api_key=DQRvPmHTEvaePSVNKMNkHgJ1a6OaP8SOpWoTSJLO 
    extract_apod = SimpleHttpOperator(
        task_id = 'extract_apod',
        http_conn_id='nasa_api', 
        endpoint = 'planetary/apod', #NASA API endpoint for APOD
        method='GET',
        data={"api_key":"{{conn.nasa_api.extra_dejson.api_key}}"}, # USE this API key
        response_filter = lambda response:response.json(), #Convert response to json
    )

    # Step 3 : Transform the data (Pick the information that we need to save)
    @task
    def transform_apod_data(response):
        apod_data = {
            'title' : response.get('title',''), # If key is not available then it will give us blank
            'explaination': response.get('explaination',''),
            'url': response.get('url',''),
            'date':response.get('date',''),
            'media_type': response.get('media_type','')
        }
        return apod_data

    # Step 4 : Loading the data into the Postgres SQL
    @task
    def load_data_to_postgres(apod_data):
        #Initialize the postgres
        postgres_hook=PostgresHook(postgres_conn_id = 'my_postgres_connection')
        # Defining the sql insert query as table is already created.
        insert_query = """
        INSERT INTO apod_data (title,explaination,url,date,media_type)
        VALUES (%s,%s,%s,%s,%s);
        """
        #Execute the sql insert statements
        postgres_hook.run(insert_query,parameters =(
            apod_data['title'],
            apod_data['explaination'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))


    # Step 5 : verify the details using DBviewer

    # Step 6 : Define the task dependencies
    create_table() >> extract_apod # extract apod is simply a http request
    # every http request has an output so we need to store that
    api_resposnse = extract_apod.output
    transformed_data = transform_apod_data(api_resposnse)
    load_data_to_postgres(transformed_data)
