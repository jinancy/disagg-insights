# disagg-insights

**download Docker**  
[https://www.docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop)  

**Spin up a PostgreSQL database and pgadmin using docker**  
Pull postgres  
`$ docker pull postgres`  
Run postgres image:  
`$ docker run -d --name postgres -e POSTGRES_PASSWORD=password -p 5433:5432 postgres`  

**Starting the pgAdmin instance**  
`docker pull dpage/pgadmin4`  
Run pgadmin4 image:  
`$ docker run -p 5555:80 -e 'PGADMIN_DEFAULT_EMAIL=[your email]' -e 'PGADMIN_DEFAULT_PASSWORD=passsowrd' --name dev-pgadmin -d dpage/pgadmin4`  

**Log into pgadmin4**  
localhost:5555  
username: your email  
password: password  

**Make a new server, database, table** 
add new server ->  
```
- Name:  
- Host name/ address: host.docker.internal  
- Port: 5433  
- username: postgres  
- password: password  
```
add database ->  
```
- name: measurement_source 
``` 
Schemas -> add table -> 
```
- name: measurement_data   
- columns: data_id, local_15min, grid  
```
             
references: 
- [Video: Spining Postgres instances and PGAdmin with Docker](https://www.youtube.com/watch?v=5QNL7_i-ay8)
- [Local Development Set-Up of PostgreSQL with Docker](https://towardsdatascience.com/local-development-set-up-of-postgresql-with-docker-c022632f13ea)
- [Video: Populate Data in Postgres](https://youtu.be/QF-qHWekhxw)

### Local Development (MAC)
- install python/pip (suggested version: 3.7.x)
  ```
  https://docs.python-guide.org/starting/install3/osx/ (manual installation)
  ```
- create python venv
```console
venv_name=venv
python3 -m venv ${venv_name}
${venv_name}/bin/pip install --upgrade pip
${venv_name}/bin/pip install -r scoring/requirements/disagg_insights.txt
${venv_name}/bin/pip install -r scoring/requirements/tests.txt
```
- configure Intellij Idea
```
* Import new/existing project 
* Go to File -> Project Structure -> SDKs -> Add Python SDK
* Choose Virtual Environment. Use existing and provide ${project}/venv/bin/python as path
* Go to Modules and set Module SDK to SDK you just created
```
