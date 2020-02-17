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

All things related to Airflow are within folder *etl*

First off, acquire an access key (a json file) from GCP to access BigQuery. Place this key into *key* folder inside *etl* folder. For example, the path to the key file is: "*./etl/key/my_access_key.json*".

