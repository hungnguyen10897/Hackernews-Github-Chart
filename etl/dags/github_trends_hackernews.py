from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator


from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2020, 2, 5),
    #'end_date' : datetime(2020, 1, 5),
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

schedule_interval = "0 0 * * *"

dag = DAG('bigquery_github_trends', catchup=True ,default_args = default_args, schedule_interval = schedule_interval)

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
# To test: docker-compose run --rm webserver airflow test bigquery_github_trends github_daily_metrics_write 2020-01-01
t3 = BigQueryOperator(
    task_id='github_daily_metrics_write',
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
# To test: docker-compose run --rm webserver airflow test bigquery_github_trends github_metrics_aggregate 2020-01-01
t4 = BigQueryOperator(
    task_id='github_metrics_aggregate',
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

# Task 5: Aggregate Hackernews data
# To test: docker-compose run --rm webserver airflow test bigquery_github_trends hackernews_aggregate 2020-01-01
t5 = BigQueryOperator(
    task_id='hackernews_aggregate',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    sql='''
    #standardSQL
    SELECT
      FORMAT_TIMESTAMP("%Y%m%d", timestamp) AS date,
      `by` AS submitter,
      id as story_id,
      REGEXP_EXTRACT(url, "(https?://github.com/[^/]*/[^/#?]*)") as url,
      SUM(score) as score
    FROM
      `bigquery-public-data.hacker_news.full`
    WHERE
      type = 'story'
      AND timestamp>'{{ yesterday_ds }}'
      AND timestamp<'{{ ds }}'
      AND url LIKE '%https://github.com%'
      AND url NOT LIKE '%github.com/blog/%'
    GROUP BY
      date, 
      submitter,
      story_id,
      url
    ''',
    destination_dataset_table=f'{GCP_PROJECT}.{PROJECT_DATASET}.hackernews_agg${r"{{ yesterday_ds_nodash }}"}',
    bigquery_conn_id= BQ_CONN,
    dag=dag
)


# Task 6: Join the GitHub and HackerNews aggregated table
# To test: docker-compose run --rm webserver airflow test bigquery_github_trends hackernews_github_join 2020-01-01
t6 = BigQueryOperator(
    task_id='hackernews_github_join',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    sql=f'''
    #standardSQL
    SELECT
      a.date as date,
      a.url as github_url,
      b.repo as github_repo,
      a.score as hn_score,
      a.story_id as hn_story_id,
      b.stars_last_28_days as stars_last_28_days,
      b.stars_last_7_days as stars_last_7_days,
      b.stars_last_1_day as stars_last_1_day,
      b.forks_last_28_days as forks_last_28_days,
      b.forks_last_7_days as forks_last_7_days,
      b.forks_last_1_day as forks_last_1_day
    FROM
    (SELECT
      *
    FROM
      `{GCP_PROJECT}.{PROJECT_DATASET}.hackernews_agg`
      WHERE _PARTITIONTIME BETWEEN TIMESTAMP("{r"{{ yesterday_ds }}"}") AND TIMESTAMP("{r"{{ yesterday_ds }}"}")
      )AS a
    LEFT JOIN
      (
      SELECT
        repo,
        CONCAT('https://github.com/', repo) as url,
        stars_last_28_days,
        stars_last_7_days,
        stars_last_1_day,
        forks_last_28_days,
        forks_last_7_days,
        forks_last_1_day
      FROM
      `{GCP_PROJECT}.{PROJECT_DATASET}.github_agg`
      WHERE _PARTITIONTIME BETWEEN TIMESTAMP("{r"{{ yesterday_ds }}"}") AND TIMESTAMP("{r"{{ yesterday_ds }}"}")
      ) as b
    ON a.url = b.url
    ''',
    destination_dataset_table=f'{GCP_PROJECT}.{PROJECT_DATASET}.hackernews_github_agg${r"{{ yesterday_ds_nodash }}"}',
    bigquery_conn_id= BQ_CONN,
    dag=dag)

# Task 7: Check for data in the final table
# To test: docker-compose run --rm webserver airflow test bigquery_github_trends check_hackernews_github_join 2020-01-01
t7 = BigQueryCheckOperator(
    task_id='check_hackernews_github_join',
    sql=f'''
    #legacySql
    SELECT
    partition_id
    FROM
    [{GCP_PROJECT}:{PROJECT_DATASET}.hackernews_github_agg$__PARTITIONS_SUMMARY__]
    WHERE partition_id = "{r"{{ yesterday_ds_nodash }}"}"
    ''',
    bigquery_conn_id= BQ_CONN,
    dag=dag)

t1.set_downstream(t3)
t3.set_downstream(t4)
t2.set_downstream(t5)
t4.set_downstream(t6)
t5.set_downstream(t6)
t6.set_downstream(t7)