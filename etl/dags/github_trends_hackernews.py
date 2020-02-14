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
GCP_PROJECT = "airflow-hackernews-github"
PROJECT_DATASET = "github_trends"

schedule_interval = "00 21 * * *"

dag = DAG('bigquery_github_trends', default_args = default_args, schedule_interval = schedule_interval)

#Task 1: Check if githubarchive contains data for the date.
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

#Task 2: Check if hackernews has data for the date.
# To test: docker-compose run --rm webserver airflow test bigquery_github_trends hackernews_full_table_check 2020-01-01
t2 = BigQueryCheckOperator(
    task_id = "hackernews_full_table_check",
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

#Task 3: Create/Load github_daily_metrics table
# To test: docker-compose run --rm webserver airflow test bigquery_github_trends write_to_github_daily_metrics 2020-01-01
t3 = BigQueryOperator(
    task_id='write_to_github_daily_metrics',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    sql='''
    #standardSQL
    SELECT
      date,
      repo,
      SUM(IF(type='WatchEvent', 1, NULL)) AS stars,
      SUM(IF(type='ForkEvent',  1, NULL)) AS forks
    FROM (
      SELECT
        FORMAT_TIMESTAMP("%Y%m%d", created_at) AS date,
        actor.id as actor_id,
        repo.name as repo,
        type
      FROM
        `githubarchive.day.{{ yesterday_ds_nodash }}`
      WHERE type IN ('WatchEvent','ForkEvent')
    )
    GROUP BY
      date,
      repo
    ''',    
    destination_dataset_table=f'{GCP_PROJECT}.{PROJECT_DATASET}.github_daily_metrics{r"${{ yesterday_ds_nodash }}"}',
    bigquery_conn_id=BQ_CONN,
    dag=dag
)

# Task 4: Aggregate Github metrics into the last 1, 7, 28 days.
# To test: docker-compose run --rm webserver airflow test bigquery_github_trends aggregate_github_metrics 2020-01-01
t4 = BigQueryOperator(
    task_id='aggregate_github_metrics',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    sql=f'''
    #standardSQL
    SELECT
        {r"{{ yesterday_ds_nodash }}"} as date,
        repo, 
        SUM(IF(_PARTITIONTIME BETWEEN TIMESTAMP("{r"{{ yesterday_ds }}"}") 
            AND TIMESTAMP("{r"{{ yesterday_ds }}"}"), stars, NULL)) AS stars_last_1_day,
        SUM(IF(_PARTITIONTIME BETWEEN TIMESTAMP("{r"{{ macros.ds_add(ds, -6) }}"}") 
            AND TIMESTAMP("{r"{{ yesterday_ds }}"}"), stars, NULL)) AS stars_last_7_days,
        SUM(IF(_PARTITIONTIME BETWEEN TIMESTAMP("{r"{{ macros.ds_add(ds, -27) }}"}") 
            AND TIMESTAMP("{r"{{ yesterday_ds }}"}"), stars, NULL)) AS stars_last_28_days,
        SUM(IF(_PARTITIONTIME BETWEEN TIMESTAMP("{r"{{ yesterday_ds }}"}") 
            AND TIMESTAMP("{r"{{ yesterday_ds }}"}"), forks, NULL)) AS forks_last_1_day,
        SUM(IF(_PARTITIONTIME BETWEEN TIMESTAMP("{r"{{ macros.ds_add(ds, -6) }}"}") 
            AND TIMESTAMP("{r"{{ yesterday_ds }}"}"), forks, NULL)) AS forks_last_7_days,
        SUM(IF(_PARTITIONTIME BETWEEN TIMESTAMP("{r"{{ macros.ds_add(ds, -27) }}"}") 
            AND TIMESTAMP("{r"{{ yesterday_ds }}"}"), forks, NULL)) AS forks_last_28_days
    FROM `{GCP_PROJECT}.{PROJECT_DATASET}.github_daily_metrics`
    WHERE _PARTITIONTIME BETWEEN TIMESTAMP("{r"{{ macros.ds_add(ds, -27) }}"}") AND TIMESTAMP("{r"{{ yesterday_ds }}"}")
    GROUP BY
      date,
      repo
    ''',    
    destination_dataset_table=f'{GCP_PROJECT}.{PROJECT_DATASET}.github_agg${r"{{ yesterday_ds_nodash }}"}',
    bigquery_conn_id=BQ_CONN,
    dag=dag
)

