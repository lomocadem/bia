# Airflow
To Start Docker containing Airflow Server: 

`docker-compose -f docker-compose.yaml up -d --build`

To stop and remove the Docker container: 

`docker-compose down`
# Note:
* Create .env file in root folder (next to docker-compose.yaml) and add variables as in .env.example.
* Default variables for user & passwords within Docker containers **should be changed in production.**
* DAGs are not designed to catch_up as it is not recommended in production, use **backfill** param when needed.
