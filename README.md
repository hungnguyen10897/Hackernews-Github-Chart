# Hackernews-Github-Chart
<br/>

The project exhibits how to use [Apache Airflow](https://airflow.apache.org/) to programmatically manage workflow. Public Datasets from GCP BigQuery (Hackernews and Github Archive) are automatically and periodically aggregated and joined to produce data for a dashboard in Google Data Studio.

All necessary CLI commands are listed in *commands* file.

<br/>

## Exploratory Data Analysis

<br/>

The two datasets used here are Hackernews and Githubarchive from GCP BigQuery public datasets. To gain some understanding of the datasets, use Jupyter notebook (in folder *EDA*) to do exploratory data analysis.

Start the container with `docker-compose up -d`

You will need a token to access the notebook, find the logs of the jupyter notebook container with `docker container logs eda_jupyter-notebook_1`, and find for this similar part.


![Access Token](https://github.com/hungnguyen10897/Hackernews-Github-Chart/blob/master/Images/access-token-jn.png)

Copy and paste the whole url toghether with the token part to the browser.

<br/>

## Workflow Management with Airflow

<br/>

All things related to Airflow are within folder *etl*. Within *etl* create a directory *pg_data* as a volume for postgres container.

First off, acquire an service key (a json file) from GCP to access BigQuery, [more detail](https://cloud.google.com/iam/docs/creating-managing-service-account-keys#iam-service-account-keys-create-console). Place this key into *key* folder inside *etl* folder. For example, the path to the key file is: "*./etl/key/my_access_key.json*".

Next is to configure connection to GCP BigQuery on airflow. This is done directly through the web UI of airflow, accessible at localhost:8080. 

Supply your GCP Query configurations in the Variables tab under Admin as a Json file.

<br/>

![GCP](https://github.com/hungnguyen10897/Hackernews-Github-Chart/blob/master/Images/Airflow-Variables.png)

<br/>

Then in the Connections tab, set up the key according to the following:

<br/>

![GCP](https://github.com/hungnguyen10897/Hackernews-Github-Chart/blob/master/Images/AirflowConnection.png)

<br/>

Replace your project_id and your keyfile name in the Keyfile Path section. For example, your keyfile name is *my_gcp_key.json* then in the Keyfile Path section: */tmp/gcp_key/my_gcp_key.json*



The *dags* folder contains the dag file for Airflow. Commands to test the tasks are provided for each task, simply replace the desired *execution_date* at the end of the CLI commands.

The dag is scheduled to run once a day. Changing the *start_date* entry for the *default_args* and initiating a dag run from Airflow Web UI will automatically perform a **catchup**, running all the tasks from the start_date (probably in the past) to present, for each day.

<br/>

![default arguments](https://github.com/hungnguyen10897/Hackernews-Github-Chart/blob/master/Images/task_arguments.png)

<br/>

## Visualization with Google Data Studio

<br/>

The output data can be visualized easily with Google Data Studio.

<br/>

![Visualizing the data](https://github.com/hungnguyen10897/Hackernews-Github-Chart/blob/master/Images/report_dashboard.png)

<br/>
