from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator


from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2020, 1, 1),
    'end_date' : datetime(2020, 1, 5),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

BQ_CONN = "gcp_conn"
schedule_interval = "00 21 * * *"

dag = DAG('bigquery_github_trends', default_args = default_args, schedule_interval = schedule_interval)

#Task 1: Check if githubarchive contains data for the date
# To test: docker-compose run --rm webserver airflow test bigquery_github_trends github_table_check 2020-01-01
t1 = BigQueryCheckOperator(
    task_id = "github_table_check",
    sql = """
        SELECT * FROM
        `githubarchive.day.__TABLES_SUMMARY__`
        WHERE table_id = '{{ yesterday_ds_nodash }}'
    """,
    bigquery_conn_id = BQ_CONN,
    use_legacy_sql = False,
    dag = dag
)

#Task 2: Check if hackernews 
# To test: docker-compose run --rm webserver airflow test bigquery_github_trends hackernews_table_check 2020-01-01
t2 = BigQueryCheckOperator(
    task_id = "hackernews_table_check",
    sql = """
        SELECT timestamp FROM
        `bigquery-public-data.hacker_news.full`
        WHERE EXTRACT(date from timestamp) = '{{ yesterday_ds }}'
            AND type = 'story'
        LIMIT 1
    """,
    bigquery_conn_id=BQ_CONN,
    use_legacy_sql=False,
    dag=dag
)