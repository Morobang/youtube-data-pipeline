from airflow import DAG
from datetime import timedelta, datetime
import pendulum
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json


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

with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description='Extract YouTube channel data via API and save to JSON',
    schedule='0 14 * * *',  # Every day at 14:00 Africa/Johannesburg
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
) as dag:

    playlist_id = get_playlist_id()
    video_ids   = get_video_ids(playlist_id)
    video_data  = extract_video_data(video_ids)
    save_to_json(video_data)
