# etl_assignment
This repo contains all the relevant scripts and configs required to create an ETL pipeline using docker,postgres and airflow.


Steps Required to run the ETL Process:-

1. Create a root directory and store all the files in it, also create empty folder ./logs  for airflow logs inside root folder

mkdir ./dags ./logs ./plugins 


2. "docker-compose.yml" file has all the configuration required to spin up different services such as source and destination postgres db, airflow scheduler , airflow webserver , and it has all the host, port and database information about the underlying services.

   Run  "docker-compose up -d" to spin up all the services 
   
   Note: use docker compose down to stop and remove all the runnning docker conntainers.
   
   
   <img width="1013" alt="Screenshot 2022-08-25 at 12 44 37 AM" src="https://user-images.githubusercontent.com/25201417/186503876-7231f9b0-fdc1-4428-bf99-a6fa3c978e4a.png">

   
3.  The source and destination postgres databases are hosted with different IPs and exposed on different port numbers - 5432 and 5433

Source DB Name : airflow
Destination DB Name : airflow_dest

4. Airflow webserver port is exposed on port : 5884 , it can beb accessed as http://localhost:5884/ (username : airflow , password : airflow)
<img width="1680" alt="Screenshot 2022-08-25 at 12 41 28 AM" src="https://user-images.githubusercontent.com/25201417/186503236-b7600f1b-ae9a-42cc-970b-7de6fd853fad.png">

5. After running the docker-compose up command, check the running status of running containers on docker with "docker ps" command

6. Now, we will enter into our docker container for source postgres database and create a new table, and insert the sample records by using the following commands:-

 docker ps
 docker exec -it [source_postgres_container_id] psql -U postgres
 
\l (list all the databses)
\c [database_name] (connect to a specific database)
\dt (list of all relations)

CREATE TABLE table_num (id INT NOT NULL, creation_date VARCHAR(250) NOT NULL, sale_value VARCHAR(250) NOT NULL); (create the table with the given schema in db)

INSERT INTO table_num (id, creation_date, sale_value) values(0, '12-12-21','1000'),(1, '13-12-21','2000'),(2, '14-12-21','3000'),
(3, '15-12-21','4000') ;  (Insert sample records)

SELECT * FROM table_num; (check and verify the records in source db)

7. Now, access the airflow and trigger the dag with dag_id = 'migrate_data'

8. The DAG on airflow perform the following steps to perform the ETL operation

- connect to source postgres instance using "psycopg2" PostgreSQL database adapter and specifying appropriate config in psycopg2 object
- create cursor object and read all the records
- insert all the records into pandas Dataframe
- connect to destination  postgres instance using the same config format as above
- store all the dataframe rows into tuples 
- Insert these tuples into destination postgres instance
- check and verify by running the sql query by entering into docker container for destination postgres db



<img width="1013" alt="Screenshot 2022-08-25 at 12 42 20 AM" src="https://user-images.githubusercontent.com/25201417/186503386-4e8a996a-dd40-4ee2-88f7-8b2fb8813d5d.png">





 



