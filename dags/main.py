from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import timedelta, datetime
import pendulum

from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json
from datawarehouse.dwh import staging_table, core_table
# from dataquality.soda import yt_elt_data_quality  # uncomment in Section 6


local_tz = pendulum.timezone("Africa/Johannesburg")

default_args = {
    'owner': 'dataengineers',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['dataengineers@example.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 6, 1, tzinfo=local_tz),
}


# DAG 1: extract YouTube data and save to JSON, then trigger DAG 2
with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description='Extract YouTube channel data via API and save to JSON',
    schedule='0 14 * * *',  # Every day at 14:00 Africa/Johannesburg
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
) as dag_produce:

    playlist_id       = get_playlist_id()
    video_ids         = get_video_ids(playlist_id)
    extract_data      = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id='trigger_update_db',
        trigger_dag_id='update_db',
    )

    playlist_id >> video_ids >> extract_data >> save_to_json_task >> trigger_update_db


# DAG 2: load JSON into bronze, transform into core, then trigger data quality
# schedule=None means this DAG only runs when triggered by produce_json above
with DAG(
    dag_id='update_db',
    default_args=default_args,
    description='Load JSON into bronze and transform into core schema',
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
) as dag_update:

    update_staging = staging_table()
    update_core    = core_table()

    # trigger_data_quality = TriggerDagRunOperator(
    #     task_id='trigger_data_quality',
    #     trigger_dag_id='data_quality',
    # )

    update_staging >> update_core
    # update_core >> trigger_data_quality  # uncomment in Section 6
