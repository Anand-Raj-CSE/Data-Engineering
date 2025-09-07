Project Information: We are pulling NASA's Astronomy Picture of the day which is an external API.
It transforms the API data and then loads the whole data into postgres Database.
We would be leveraging Docker to run Airflow and postgres in a container.
We would be using Airflow hooks to push data into postgres.
Earlier we used tasks using PythonOperator or @task decorator but here we would be using HTTPOperator for API integration.
Transformation would be a generic python code.
We would use docker compose as airflow would be runnig on a different docker container and postgres would be runnign in a different docker container , so both the containers to interact we need to create a docker compose yml file for seemless integration .
For loading we would use postgres hook , for S3 we have S3 hook .
